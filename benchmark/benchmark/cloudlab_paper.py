"""Vertex-payload sweep using the Angelfish paper's experimental method."""

import asyncio
import json
import os
import shlex
import shutil
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path

from benchmark.cloudlab_remote import CloudLabBench
from benchmark.logs import LogParser, ParseError
from benchmark.utils import PathMaker, Print


PAPER_URL = 'https://arxiv.org/html/2509.15847v1#S5'
SCRIPT_URL = 'https://github.com/qyu100/sf-dag/blob/angelfish/benchmark/run.sh'
HEADER_SIZES = '32000,64000,128000,256000,512000,768000'


def commit_metrics(logger, parsed_primaries, configured_duration=None):
    """Keep first-commit metrics and also measure commit at every node.

    Payload generation has no transaction timestamps in this implementation;
    the Created-header timestamp is explicitly a proxy for creation time.
    Empty vertices carry no transactions and get zero latency weight.
    """
    commits_by_node = [parsed[1] for parsed in parsed_primaries]
    common = set.intersection(*(set(commits) for commits in commits_by_node))
    common &= logger.proposals.keys()
    payload_headers = {d for d in common if logger.sizes.get(d, 0) > 0}
    latest = {d: max(commits[d] for commits in commits_by_node) for d in common}
    tx_size = logger.configs[0]['transaction_size']
    total_bytes = sum(logger.sizes[d] for d in payload_headers)
    start = min(logger.proposals.values()) if logger.proposals else None
    duration = (max(latest[d] for d in payload_headers) - start
                if payload_headers and start is not None else 0)
    first_payload = {d for d in logger.commits if logger.sizes.get(d, 0) > 0}
    first_bytes = sum(logger.sizes[d] for d in first_payload)
    valid = bool(payload_headers and duration > 0)
    if configured_duration is not None:
        valid = valid and duration >= 0.9 * configured_duration
    return {
        'valid_for_comparison': valid,
        'invalid_reason': None if valid else 'No all-node payload commits, or all-node commit activity covers less than 90% of configured duration',
        'latency_start': 'Created-header log timestamp (transaction creation proxy)',
        'latency_weighting': 'payload bytes; empty vertices excluded',
        'all_nodes_payload_vertices': len(payload_headers),
        'all_nodes_transactions': total_bytes // tx_size,
        'all_nodes_observed_duration_s': duration,
        'all_nodes_throughput_tx_s': total_bytes / tx_size / duration if valid else None,
        'all_nodes_throughput_configured_duration_tx_s': total_bytes / tx_size / configured_duration if configured_duration else None,
        'all_nodes_creation_proxy_latency_ms': 1000 * sum(
            logger.sizes[d] * (latest[d] - logger.proposals[d])
            for d in payload_headers
        ) / total_bytes if valid else None,
        'first_node_creation_proxy_latency_ms': 1000 * sum(
            logger.sizes[d] * (logger.commits[d] - logger.proposals[d])
            for d in first_payload
        ) / first_bytes if first_bytes else None,
        'legacy_consensus_latency_ms': logger._consensus_latency() * 1000 if logger.proposals and logger.commits else None,
        'legacy_consensus_throughput_tx_s': logger._consensus_only_throughput() if logger.proposals and logger.commits else None,
        'per_node_committed_vertices': [len(commits) for commits in commits_by_node],
    }


class PaperCloudLabBench(CloudLabBench):
    def __init__(self, ctx, header_sizes, output_dir, protocol, resume=False):
        super().__init__(ctx)
        self.header_sizes = header_sizes
        self.output_dir = Path(output_dir).expanduser().resolve()
        self.protocol = protocol
        self.resume = resume

    async def _run(self, hosts, bench, params, debug, consensus_only, update, propose_rates):
        if not consensus_only or len(bench.nodes) != 1:
            raise ValueError('Paper sweep requires consensus-only and one committee size')
        initial_manifest = self.output_dir / 'manifest.json'
        if self.resume and initial_manifest.exists():
            previous_bench = json.loads(initial_manifest.read_text())['benchmark']
            for key in ('tx_size', 'duration', 'nodes', 'faults'):
                if previous_bench[key] != vars(bench)[key]:
                    raise ValueError(f'Resume would mix different {key}; use a new output directory')
        self.output_dir.mkdir(parents=True, exist_ok=self.resume)
        attempt_tag = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
        manifest_path = self.output_dir / (
            f'resume-manifest-{attempt_tag}.json' if self.resume else 'manifest.json'
        )
        old_logs = Path(PathMaker.logs_path())
        if old_logs.exists():
            shutil.copytree(old_logs, self.output_dir / (
                f'previous-final-logs-{attempt_tag}' if self.resume else 'previous-final-logs'
            ))
        manifest = {
            'protocol': self.protocol,
            'started_at': datetime.now(timezone.utc).isoformat(),
            'local_commit': subprocess.check_output(
                ['git', 'rev-parse', 'HEAD'], text=True).strip(),
            'paper_url': PAPER_URL,
            'payload_grid_source': SCRIPT_URL,
            'header_sizes_bytes': self.header_sizes,
            'benchmark': vars(bench),
            'node_parameters_initial': dict(params.json),
            'propose_rates': propose_rates,
            'hosts': hosts,
            'method': 'synthetic payload generated at each vertex; no external clients',
            'differences_from_paper': [
                'Available CloudLab committee and network replace 50 nodes in 5 GCP regions',
                'Existing generator uses zero bytes instead of random transaction bytes',
                'Created-header timestamp proxies transaction creation; no timestamp inside synthetic transactions',
                'Payload grid follows released run.sh; timeout follows paper text; transaction size and duration follow user settings',
                f'User-configured transaction size={bench.tx_size} B, duration={bench.duration} s and repetitions={bench.runs}; paper text specifies 512 B and 180 s',
            ],
        }
        manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
        connections = await self._connect_all(hosts)
        try:
            committee, names = self._generate_config(hosts, params, bench)
            if update:
                await self._run_scripts('Update', connections,
                                        lambda _: self._repository_script('paper update'))
            remote_metadata = await asyncio.gather(*(
                connection.run(
                    'git -C ' + shlex.quote(self.settings.repo_name) +
                    ' rev-parse HEAD; readlink "$HOME/node"; date -u +%FT%T.%NZ',
                    check=True,
                ) for connection in connections.values()
            ))
            manifest['remote_commit_binary_time'] = {
                host: result.stdout.strip().splitlines()
                for host, result in zip(connections, remote_metadata)
            }
            manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
            await self._upload_all(committee, names, connections, bench.faults)
            for propose_rate in propose_rates:
                params.json['propose_rate'] = propose_rate
                for header_size in self.header_sizes:
                    params.json['header_size'] = header_size
                    params.json['batch_size'] = header_size
                    await self._upload_parameters(hosts, connections, params)
                    for run_index in range(bench.runs):
                        point = self.output_dir / (
                            f'pr-{propose_rate:g}/header-{header_size}/run-{run_index + 1}'
                        )
                        if self.resume and point.exists():
                            existing = list(point.rglob('metrics.json'))
                            def is_valid(path):
                                old = json.loads(path.read_text())
                                return old.get('valid_for_comparison',
                                               old.get('all_nodes_observed_duration_s', 0) >= 0.9 * bench.duration)
                            if any(is_valid(path) for path in existing):
                                Print.info(f'Skipping completed valid point: {point}')
                                continue
                            point = point / f'retry-{attempt_tag}'
                        point.mkdir(parents=True)
                        params.print(str(point / 'parameters.json'))
                        shutil.copyfile(PathMaker.committee_file(), point / 'committee.json')
                        Print.heading(
                            f'{self.protocol} paper sweep: {len(hosts)} nodes, '
                            f'propose_rate={propose_rate:g}, header={header_size} B, '
                            f'{header_size // bench.tx_size} tx/vertex, run={run_index + 1}'
                        )
                        await self._run_single(0, bench.burst[0], committee, bench,
                                               connections, debug, True)
                        await self._download_logs(True, committee, bench.faults, connections)
                        shutil.copytree(PathMaker.logs_path(), point / 'logs')
                        logger = LogParser.process(str(point / 'logs'), bench.burst[0],
                                                   faults=bench.faults, consensus_only=True)
                        parsed = [logger._parse_primaries(path.read_text())
                                  for path in sorted((point / 'logs').glob('primary-*.log'))]
                        metrics = commit_metrics(logger, parsed, bench.duration)
                        metrics.update(protocol=self.protocol, nodes=len(hosts),
                                       propose_rate=propose_rate, header_size_bytes=header_size,
                                       transactions_per_vertex=header_size // bench.tx_size,
                                       run=run_index + 1, duration_s=bench.duration,
                                       transaction_size_bytes=bench.tx_size)
                        (point / 'metrics.json').write_text(json.dumps(metrics, indent=2) + '\n')
                        # result() appends a legacy CSV; keep it with this experiment.
                        previous = Path.cwd()
                        try:
                            os.chdir(self.output_dir)
                            summary = logger.result() if logger.proposals and logger.commits else 'INVALID: no proposals or commits\n'
                        finally:
                            os.chdir(previous)
                        (point / 'summary.txt').write_text(summary + '\n' + json.dumps(metrics, indent=2) + '\n')
                        with tarfile.open(point / 'logs.tar.gz', 'w:gz') as archive:
                            archive.add(point / 'logs', arcname='logs')
                        shutil.rmtree(point / 'logs')
                        with (self.output_dir / 'metrics.jsonl').open('a') as output:
                            output.write(json.dumps(metrics) + '\n')
                        print(summary, flush=True)
                        if metrics['valid_for_comparison']:
                            print('All-node TPS: {:.0f}; payload-weighted creation-proxy latency: {:.2f} ms'.format(
                                metrics['all_nodes_throughput_tx_s'], metrics['all_nodes_creation_proxy_latency_ms']), flush=True)
                        else:
                            Print.warn(f'Invalid measurement retained: {metrics["invalid_reason"]}')
                        print(f'Result directory: {point}', flush=True)
            (self.output_dir / 'COMPLETE').write_text(datetime.now(timezone.utc).isoformat() + '\n')
        except BaseException:
            await self._kill(connections)
            raise
        finally:
            await self._close_all(connections)

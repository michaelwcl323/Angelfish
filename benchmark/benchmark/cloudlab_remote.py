# Copyright(C) Facebook, Inc. and its affiliates.
"""CloudLab-only implementation of Angelfish's remote benchmark workflow."""

import asyncio
import os
import shlex
import shutil
import subprocess
from collections import OrderedDict
from copy import deepcopy
from itertools import zip_longest
from math import ceil
from pathlib import Path

import asyncssh

from benchmark.cloudlab_instance import CloudLabInstanceManager
from benchmark.commands import CommandMaker
from benchmark.config import (
    BenchParameters,
    BlsKey,
    Committee,
    ConfigError,
    EdKey,
    NodeParameters,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import BenchError, PathMaker, Print, progress_bar


class CloudLabExecutionError(Exception):
    """A command or transfer failed on one or more CloudLab nodes."""


class CloudLabBench:
    """Install and benchmark Angelfish exclusively on CloudLab nodes."""

    # Angelfish's benchmark client sends ``rate / 20`` transactions per
    # burst. The CloudLab runner converts a target tx/s rate into that legacy
    # command-line representation without changing benchmark_client.rs.
    CLIENT_PRECISION = 20

    def __init__(self, ctx=None, settings_file='cloudlab_settings.json'):
        del ctx
        self.manager = CloudLabInstanceManager.make(settings_file)
        self.settings = self.manager.settings
        self.key_path = Path(self.settings.key_path).expanduser()
        if not self.key_path.is_file():
            raise BenchError(
                'Failed to initialize CloudLab',
                FileNotFoundError(
                    f'SSH private key not found: {self.key_path}'
                ),
            )

    def _connection_options(self, host):
        info = self.manager.host(host)
        options = {
            'client_keys': [str(self.key_path)],
            'connect_timeout': 30,
            'keepalive_interval': 10,
            'keepalive_count_max': 60,
            'known_hosts': None,
            'login_timeout': 30,
            'username': info['username'],
            'port': info['port'],
        }
        password = (
            os.environ.get('SSH_KEY_PASSWORD')
            or self.settings.key_password
        )
        if password:
            options['passphrase'] = password
        return options

    async def _connect_one(self, host, retries=5):
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                connection = await asyncssh.connect(
                    host, **self._connection_options(host)
                )
                return host, connection
            except Exception as error:
                last_error = error
                if attempt < retries:
                    await asyncio.sleep(1)
        return host, ConnectionError(
            f'failed to connect after {retries} attempts: {last_error}'
        )

    async def _connect_all(self, hosts):
        results = await asyncio.gather(
            *(self._connect_one(host) for host in hosts)
        )
        failures = [
            (host, result)
            for host, result in results
            if isinstance(result, Exception)
        ]
        if failures:
            for _, result in results:
                if not isinstance(result, Exception):
                    result.close()
            details = '; '.join(
                f'{host}: {error}' for host, error in failures
            )
            raise CloudLabExecutionError(
                f'CloudLab SSH connection failed: {details}'
            )
        return {host: connection for host, connection in results}

    @staticmethod
    async def _close_all(connections):
        for connection in connections.values():
            connection.close()
        await asyncio.gather(
            *(connection.wait_closed() for connection in connections.values()),
            return_exceptions=True,
        )

    @staticmethod
    def _result_error(host, result):
        stderr = (result.stderr or '').strip()
        stdout = (result.stdout or '').strip()
        details = stderr or stdout or 'no command output'
        return CloudLabExecutionError(
            f'{host} exited with status {result.exit_status}: {details}'
        )

    async def _run_script_one(self, host, connection, script):
        try:
            command = f'bash -lc {shlex.quote(script)}'
            result = await connection.run(command, check=False)
            if result.exit_status != 0:
                return host, self._result_error(host, result)
            return host, result
        except Exception as error:
            return host, CloudLabExecutionError(f'{host}: {error}')

    async def _run_scripts(self, label, connections, script_factory):
        results = await asyncio.gather(*(
            self._run_script_one(
                host,
                connection,
                script_factory(host),
            )
            for host, connection in connections.items()
        ))
        failures = [
            (host, result)
            for host, result in results
            if isinstance(result, Exception)
        ]
        for host, result in results:
            if not isinstance(result, Exception):
                Print.info(f'[{label}: OK] {host}')
        if failures:
            details = '; '.join(
                f'{host}: {error}' for host, error in failures
            )
            raise CloudLabExecutionError(f'{label} failed: {details}')
        return results

    def _repository_script(self, action):
        repo_url = shlex.quote(self.settings.repo_url)
        repo_name = shlex.quote(self.settings.repo_name)
        branch = shlex.quote(self.settings.branch)
        return f'''
set -euo pipefail
REPO_URL={repo_url}
REPO_NAME={repo_name}
BRANCH={branch}

if [ -f "$HOME/.cargo/env" ]; then
    source "$HOME/.cargo/env"
fi

for command in cargo rustc clang cmake git tmux; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing command: $command; run fab cloudlab-install" >&2
        exit 1
    fi
done

if [ ! -d "$HOME/$REPO_NAME/.git" ]; then
    if [ -e "$HOME/$REPO_NAME" ]; then
        echo "$HOME/$REPO_NAME exists but is not a Git repository" >&2
        exit 1
    fi
    git clone --branch "$BRANCH" "$REPO_URL" "$HOME/$REPO_NAME"
fi

git -C "$HOME/$REPO_NAME" fetch origin "$BRANCH"
git -C "$HOME/$REPO_NAME" checkout "$BRANCH"
git -C "$HOME/$REPO_NAME" pull --ff-only origin "$BRANCH"

cd "$HOME/$REPO_NAME/node"
cargo build --quiet --release --features benchmark
ln -sfn "$HOME/$REPO_NAME/target/release/node" "$HOME/node"
ln -sfn "$HOME/$REPO_NAME/target/release/benchmark_client" \
    "$HOME/benchmark_client"
mkdir -p "$HOME/logs" "$HOME/results"
echo "{action} complete"
'''

    def install(self):
        """Clone/update and compile Angelfish on every CloudLab node."""
        try:
            asyncio.run(self._install())
        except BenchError:
            raise
        except Exception as error:
            raise BenchError(
                'CloudLab installation failed', error
            ) from error

    async def _install(self):
        hosts = self.manager.hosts(flat=True)
        if not hosts:
            raise CloudLabExecutionError('No CloudLab hosts configured')

        Print.heading(
            f'Installing Angelfish on {len(hosts)} CloudLab node(s)'
        )
        connections = await self._connect_all(hosts)
        try:
            await self._run_scripts(
                'Install',
                connections,
                lambda _: self._repository_script('install'),
            )
        finally:
            await self._close_all(connections)
        Print.heading(
            f'Initialized CloudLab testbed of {len(hosts)} node(s)'
        )

    def _select_hosts(self, bench_parameters):
        if not bench_parameters.collocate:
            raise ConfigError(
                'CloudLab currently requires collocate=True'
            )

        required = max(bench_parameters.nodes)
        grouped_hosts = list(self.manager.hosts().values())
        ordered = [
            host
            for row in zip_longest(*grouped_hosts)
            for host in row
            if host is not None
        ]
        if len(ordered) < required:
            raise ConfigError(
                f'CloudLab has {len(ordered)} host(s), '
                f'but the benchmark requires {required}'
            )
        return ordered[:required]

    @staticmethod
    def _replace_binary_links():
        binary_dir = Path(PathMaker.binary_path())
        for name in ('node', 'benchmark_client'):
            source = binary_dir / name
            if not source.is_file():
                raise FileNotFoundError(
                    f'compiled binary not found: {source}'
                )
            target = Path(name)
            if target.is_symlink() or target.is_file():
                target.unlink()
            elif target.exists():
                raise IsADirectoryError(
                    f'cannot replace local binary link: {target}'
                )
            target.symlink_to(source)

    def _generate_config(
        self,
        hosts,
        node_parameters,
        bench_parameters,
    ):
        Print.info('Generating CloudLab configuration files locally...')

        subprocess.run(
            [CommandMaker.cleanup()],
            shell=True,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            CommandMaker.compile().split(),
            check=True,
            cwd=PathMaker.node_crate_path(),
        )
        self._replace_binary_links()

        f_num = int(node_parameters.json['f_num'])
        quorum = 2 * f_num + 1
        if f_num < 0 or quorum > len(hosts):
            raise ConfigError(
                f'f_num={f_num} requires quorum={quorum}, '
                f'but only {len(hosts)} CloudLab host(s) were selected'
            )

        # SecretKeySet threshold t requires t+1 shares. Match the 2f+1
        # certificate quorum used by Angelfish.
        threshold = 2 * f_num
        subprocess.run(
            CommandMaker.generate_bls_keys(
                len(hosts),
                threshold,
                PathMaker.bls_file_default_path(),
            ).split(),
            check=True,
        )

        keys = []
        for index in range(len(hosts)):
            filename = PathMaker.ed_key_file(index)
            subprocess.run(
                CommandMaker.generate_ed_key(filename).split(),
                check=True,
            )
            keys.append(EdKey.from_file(filename))

        bls_keys = [
            BlsKey.from_file(PathMaker.bls_key_file(index))
            for index in range(len(hosts))
        ]
        names = [key.name for key in keys]
        bls_public_keys = [key.nameg2 for key in bls_keys]

        addresses = OrderedDict(
            (name, [host] * (bench_parameters.workers + 1))
            for name, host in zip(names, hosts)
        )
        committee = Committee.from_address_list(
            addresses,
            self.settings.base_port,
            bench_parameters.faults,
            bls_public_keys,
        )
        committee.print(PathMaker.committee_file())
        node_parameters.print(PathMaker.parameters_file())
        return committee, names

    @staticmethod
    async def _upload_one(host, connection, index):
        try:
            cleanup = '''
set -e
cd "$HOME"
rm -rf .db-*
rm -f .committee.json .parameters.json .node-*.json .node-bls-*.json
mkdir -p logs results
'''
            result = await connection.run(
                f'bash -lc {shlex.quote(cleanup)}',
                check=False,
            )
            if result.exit_status != 0:
                return host, CloudLabBench._result_error(host, result)

            files = (
                PathMaker.committee_file(),
                PathMaker.bls_key_file(index),
                PathMaker.ed_key_file(index),
                PathMaker.parameters_file(),
            )
            async with connection.start_sftp_client() as sftp:
                for filename in files:
                    await sftp.put(filename, Path(filename).name)
            return host, None
        except Exception as error:
            return host, CloudLabExecutionError(
                f'failed to upload configuration to {host}: {error}'
            )

    async def _upload_all(self, committee, names, connections, faults):
        active_names = names[:len(names) - faults] if faults else names
        tasks = []
        for index, name in enumerate(active_names):
            host = next(iter(committee.ips(name)))
            tasks.append(
                self._upload_one(host, connections[host], index)
            )
        results = await asyncio.gather(*tasks)
        failures = [
            (host, result)
            for host, result in results
            if isinstance(result, Exception)
        ]
        if failures:
            details = '; '.join(
                f'{host}: {error}' for host, error in failures
            )
            raise CloudLabExecutionError(
                f'CloudLab configuration upload failed: {details}'
            )
        Print.info(
            f'Uploaded configuration to {len(results)} CloudLab node(s)'
        )

    @staticmethod
    async def _start_process(host, connection, command, log_file):
        try:
            session = Path(log_file).stem
            inner = (
                f'cd "$HOME" && exec {command} '
                f'> {shlex.quote(log_file)} 2>&1'
            )
            tmux_command = (
                f'tmux new-session -d -s {shlex.quote(session)} '
                f'bash -lc {shlex.quote(inner)}'
            )
            result = await connection.run(tmux_command, check=False)
            if result.exit_status != 0:
                return host, CloudLabBench._result_error(host, result)
            return host, result
        except Exception as error:
            return host, CloudLabExecutionError(
                f'failed to start {Path(log_file).stem} on {host}: {error}'
            )

    @staticmethod
    async def _require_success(label, tasks):
        results = await asyncio.gather(*tasks)
        failures = [
            (host, result)
            for host, result in results
            if isinstance(result, Exception)
        ]
        if failures:
            details = '; '.join(
                f'{host}: {error}' for host, error in failures
            )
            raise CloudLabExecutionError(f'{label} failed: {details}')
        return results

    async def _run_primaries(
        self,
        committee,
        connections,
        faults,
        debug,
    ):
        Print.info('Booting primaries...')
        tasks = []
        for index, address in enumerate(
            committee.primary_addresses(faults)
        ):
            host = Committee.ip(address)
            command = CommandMaker.run_primary(
                PathMaker.ed_key_file(index),
                PathMaker.bls_key_file(index),
                PathMaker.committee_file(),
                PathMaker.db_path(index),
                PathMaker.parameters_file(),
                debug=debug,
            )
            tasks.append(self._start_process(
                host,
                connections[host],
                command,
                PathMaker.primary_log_file(index),
            ))
        await self._require_success('Boot primaries', tasks)

    def _client_rate_argument(self, target_rate, burst_ms):
        transactions_per_burst = max(
            1,
            ceil(target_rate * burst_ms / 1_000),
        )
        return transactions_per_burst * self.CLIENT_PRECISION

    async def _run_clients(
        self,
        target_rate,
        burst,
        committee,
        bench_parameters,
        connections,
    ):
        Print.info('Booting clients...')
        workers = committee.workers_addresses(
            bench_parameters.faults
        )
        client_count = sum(len(addresses) for addresses in workers)
        if client_count == 0:
            raise CloudLabExecutionError(
                'The CloudLab committee has no client endpoints'
            )

        target_share = ceil(target_rate / client_count)
        client_rate = self._client_rate_argument(
            target_share,
            burst,
        )
        all_addresses = [
            address
            for addresses in workers
            for _, address in addresses
        ]
        tasks = []
        for index, addresses in enumerate(workers):
            for worker_id, address in addresses:
                host = Committee.ip(address)
                command = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    burst,
                    client_rate,
                    all_addresses,
                )
                tasks.append(self._start_process(
                    host,
                    connections[host],
                    command,
                    PathMaker.client_log_file(
                        index,
                        int(worker_id),
                    ),
                ))
        await self._require_success('Boot clients', tasks)

    async def _kill(self, connections, delete_logs=False):
        cleanup = (
            'rm -rf "$HOME/logs" && mkdir -p "$HOME/logs"'
            if delete_logs
            else 'true'
        )
        script = f'''
tmux kill-server 2>/dev/null || true
pkill -x node 2>/dev/null || true
pkill -x benchmark_client 2>/dev/null || true
{cleanup}
'''
        await self._run_scripts(
            'Cleanup' if delete_logs else 'Stop',
            connections,
            lambda _: script,
        )

    async def _run_single(
        self,
        rate,
        burst,
        committee,
        bench_parameters,
        connections,
        debug,
        consensus_only,
    ):
        await self._kill(connections, delete_logs=True)
        await self._run_primaries(
            committee,
            connections,
            bench_parameters.faults,
            debug,
        )
        if not consensus_only:
            await self._run_clients(
                rate,
                burst,
                committee,
                bench_parameters,
                connections,
            )

        duration = bench_parameters.duration
        for _ in progress_bar(
            range(20),
            prefix=f'Running benchmark ({duration} sec):',
        ):
            await asyncio.sleep(duration / 20)
        await self._kill(connections)

    @staticmethod
    async def _download_one(host, connection, remote, local):
        try:
            async with connection.start_sftp_client() as sftp:
                await sftp.get(remote, localpath=local)
            return host, None
        except Exception as error:
            return host, CloudLabExecutionError(
                f'failed to download {remote} from {host}: {error}'
            )

    async def _download_logs(
        self,
        consensus_only,
        committee,
        faults,
        connections,
    ):
        logs_path = Path(PathMaker.logs_path())
        if logs_path.exists():
            shutil.rmtree(logs_path)
        logs_path.mkdir(parents=True)

        tasks = []
        Print.info('Downloading primary logs...')
        for index, address in enumerate(
            committee.primary_addresses(faults)
        ):
            host = Committee.ip(address)
            filename = PathMaker.primary_log_file(index)
            tasks.append(self._download_one(
                host,
                connections[host],
                filename,
                filename,
            ))

        if not consensus_only:
            Print.info('Downloading client logs...')
            for index, addresses in enumerate(
                committee.workers_addresses(faults)
            ):
                for worker_id, address in addresses:
                    host = Committee.ip(address)
                    filename = PathMaker.client_log_file(
                        index,
                        int(worker_id),
                    )
                    tasks.append(self._download_one(
                        host,
                        connections[host],
                        filename,
                        filename,
                    ))
        await self._require_success('Download logs', tasks)

    @staticmethod
    def _parse_and_store(
        burst,
        rate,
        nodes,
        bench_parameters,
        consensus_only,
    ):
        Print.info('Parsing logs and computing performance...')
        logger = LogParser.process(
            PathMaker.logs_path(),
            burst,
            faults=bench_parameters.faults,
            consensus_only=consensus_only,
        )
        if not logger.proposals:
            raise ParseError(
                'No headers were proposed; inspect logs/primary-*.log'
            )
        if not logger.commits:
            raise ParseError(
                'No headers were committed; inspect logs/primary-*.log'
            )

        # benchmark_client.rs logs its legacy internal burst rate. The user
        # supplied aggregate target is the CloudLab benchmark input rate.
        if not consensus_only:
            logger.rate = (rate,)

        summary = logger.result()
        result_path = Path(PathMaker.result_file(
            bench_parameters.faults,
            nodes,
            bench_parameters.workers,
            bench_parameters.collocate,
            rate,
            bench_parameters.tx_size,
        ))
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with result_path.open('a') as output:
            output.write(summary)
        print(summary)

    async def _run(
        self,
        hosts,
        bench_parameters,
        node_parameters,
        debug,
        consensus_only,
        update,
    ):
        connections = await self._connect_all(hosts)
        try:
            committee, names = self._generate_config(
                hosts,
                node_parameters,
                bench_parameters,
            )
            if update:
                Print.info(
                    'Updating and compiling Angelfish on CloudLab nodes...'
                )
                await self._run_scripts(
                    'Update',
                    connections,
                    lambda _: self._repository_script('update'),
                )
            await self._upload_all(
                committee,
                names,
                connections,
                bench_parameters.faults,
            )

            for nodes in bench_parameters.nodes:
                committee_copy = deepcopy(committee)
                committee_copy.remove_nodes(
                    committee.size() - nodes
                )
                for burst in bench_parameters.burst:
                    rate = bench_parameters.rate[0]
                    Print.heading(
                        f'Running {nodes} CloudLab nodes '
                        f'(input rate: {rate:,} tx/s, '
                        f'burst: {burst:,} ms)'
                    )
                    for run_index in range(bench_parameters.runs):
                        Print.heading(
                            f'Run {run_index + 1}/'
                            f'{bench_parameters.runs}'
                        )
                        try:
                            await self._run_single(
                                rate,
                                burst,
                                committee_copy,
                                bench_parameters,
                                connections,
                                debug,
                                consensus_only,
                            )
                            await self._download_logs(
                                consensus_only,
                                committee_copy,
                                bench_parameters.faults,
                                connections,
                            )
                            self._parse_and_store(
                                burst,
                                rate,
                                nodes,
                                bench_parameters,
                                consensus_only,
                            )
                        except Exception:
                            await self._kill(connections)
                            raise
        finally:
            await self._close_all(connections)

    def run(
        self,
        bench_parameters_dict,
        node_parameters_dict,
        debug=False,
        consensus_only=False,
        update=True,
    ):
        """Run the original remote workflow against CloudLab only."""
        Print.heading('Starting CloudLab benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
            if not isinstance(bench_parameters.burst, list):
                bench_parameters.burst = [bench_parameters.burst]
            if any(
                not isinstance(value, int) or value <= 0
                for value in bench_parameters.burst
            ):
                raise ConfigError(
                    'Burst values must be positive integers'
                )
            selected_hosts = self._select_hosts(bench_parameters)
            asyncio.run(self._run(
                selected_hosts,
                bench_parameters,
                node_parameters,
                bool(debug),
                bool(consensus_only),
                bool(update),
            ))
        except BenchError:
            raise
        except Exception as error:
            raise BenchError(
                'CloudLab benchmark failed', error
            ) from error

    def kill(self):
        """Stop Angelfish processes on all configured CloudLab nodes."""
        try:
            asyncio.run(self._kill_all())
        except Exception as error:
            raise BenchError(
                'Failed to stop CloudLab benchmark', error
            ) from error

    async def _kill_all(self):
        hosts = self.manager.hosts(flat=True)
        connections = await self._connect_all(hosts)
        try:
            await self._kill(connections)
        finally:
            await self._close_all(connections)

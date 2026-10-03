# Copyright(C) Facebook, Inc. and its affiliates.
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import BenchError, Print


def _gcp_instance_manager():
    from benchmark.instance import InstanceManager
    return InstanceManager


def _gcp_bench():
    from benchmark.remote import Bench
    return Bench


def _cloudlab_bench():
    from benchmark.cloudlab_remote import CloudLabBench
    return CloudLabBench


@task
def local(ctx, debug=False, consensus_only=True, header_size=512_000):
    ''' Run benchmarks on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 10,
        'workers': 1,
        'rate': 100_000,
        'tx_size': 512,
        'duration': 10,
        "burst" : 50
    }
    node_params = {
        'consensus_only': consensus_only,
        'header_size': header_size,  # bytes
        'max_header_delay': 1_000,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': header_size,  # bytescd
        'tx_size': bench_params['tx_size'],
        'max_batch_delay': 200,  # ms
        'f_num': 3,  # number of faulty nodes
        'propose_rate': 0.7,
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug, consensus_only)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    try:
        _gcp_instance_manager().make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    try:
        _gcp_instance_manager().make().delete_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx):
    ''' Start at most `max` machines per data center '''
    try:
        _gcp_instance_manager().make().start_instances()
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    try:
        _gcp_instance_manager().make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    try:
        _gcp_instance_manager().make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    try:
        _gcp_bench()(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(ctx, burst = 50, debug=False, consensus_only=False, header_size=512, propose_rate=1):
    ''' Run benchmarks on GCP '''
    bench_params = {
        'faults': 0,
        'nodes': 100,
        'workers': 1,
        'collocate': True,
        'rate': [100000],
        'tx_size': 512,
        'duration': 60,
        'runs': 1,
        'burst' : [burst],
    }

    nodes = bench_params['nodes']
    rate =  1000 * nodes * 20
    bench_params['rate'] = [rate]

    node_params = {
        'consensus_only': consensus_only,
        'header_size': header_size,  # bytes
        'max_header_delay': 5_000,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': header_size,
        'tx_size': bench_params['tx_size'],  # bytes
        'max_batch_delay': 200,  # ms
        'leaders_per_round': 67,
        'f_num': 3,
        'propose_rate': float(propose_rate),
    }
    try:
        _gcp_bench()(ctx).run(bench_params, node_params, debug, consensus_only)
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_info(ctx):
    ''' Display connect information about all CloudLab nodes '''
    from benchmark.cloudlab_instance import CloudLabInstanceManager

    try:
        CloudLabInstanceManager.make().print_info()
    except BenchError as error:
        Print.error(error)


@task
def cloudlab_test(ctx):
    ''' Test SSH connections to all CloudLab nodes '''
    try:
        _cloudlab_bench()(ctx).test_connections()
    except BenchError as error:
        Print.error(error)


@task
def cloudlab_install(ctx, max_workers=8):
    ''' Initialize CloudLab and install Sailfish on all CloudLab nodes '''
    from environment_setup import (
        EnvironmentSetupError,
        setup_environment,
    )

    try:
        # Prepare this local controller and every configured CloudLab node.
        setup_environment(max_workers=int(max_workers))
        # Import AsyncSSH only after the local Python setup is complete.
        _cloudlab_bench()(ctx).install()
    except EnvironmentSetupError as error:
        Print.error(BenchError(
            'CloudLab environment setup failed',
            error,
        ))
    except BenchError as error:
        Print.error(error)


def _parse_int_list(value, default):
    '''Parse a comma/space-separated int list for Fabric CLI args.'''
    if value is None:
        return list(default)
    if isinstance(value, (list, tuple)):
        return [int(x) for x in value]
    text = str(value).strip()
    if not text:
        return list(default)
    parts = [part for part in text.replace(' ', ',').split(',') if part]
    return [int(part) for part in parts]


def _parse_float_list(value, default):
    '''Parse a comma/space-separated float list for Fabric CLI args.'''
    if value is None:
        return list(default)
    if isinstance(value, (list, tuple)):
        return [float(x) for x in value]
    text = str(value).strip()
    if not text:
        return list(default)
    parts = [part for part in text.replace(' ', ',').split(',') if part]
    return [float(part) for part in parts]


@task
def cloudlab_remote(
    ctx,
    rates='20000,40000,60000,80000,100000,120000',
    propose_rates='0.4,1',
    runs=2,
    duration=120,
    burst=50,
    nodes=10,
    debug=False,
    consensus_only=False,
    update=True,
):
    ''' Run CloudLab benchmarks; rates/propose-rates may be comma-separated. '''
    from environment_setup import (
        EnvironmentSetupError,
        setup_local_environment,
    )

    # Align with ISS CloudLab matrix: 500 B txs, 4096-tx batches, 1 s delay.
    tx_size = 500
    batch_requests = 4096
    header_size = 1_000
    try:
        setup_local_environment(check_only=True)

        rate_list = _parse_int_list(
            rates, [20000, 40000, 60000, 80000, 100000, 120000]
        )
        propose_rate_list = _parse_float_list(propose_rates, [0.4, 1.0])
        if not rate_list or any(rate <= 0 for rate in rate_list):
            raise ValueError('rates must be a non-empty list of positive integers')
        if not propose_rate_list or any(
            not 0 < value <= 1 for value in propose_rate_list
        ):
            raise ValueError('propose-rates must be in (0, 1]')
        runs = int(runs)
        duration = int(duration)
        burst = int(burst)
        nodes = int(nodes)
        if nodes <= 1:
            raise ValueError('nodes must be greater than one')
        if burst <= 0:
            raise ValueError('burst must be greater than zero')
        if duration <= 0:
            raise ValueError('duration must be greater than zero')
        if runs <= 0:
            raise ValueError('runs must be greater than zero')

        f_num = (nodes - 1) // 3
        bench_params = {
            'faults': 0,
            'nodes': [nodes],
            'workers': 1,
            'collocate': True,
            'rate': rate_list,
            'tx_size': tx_size,
            'duration': duration,
            'runs': runs,
            'burst': [burst],
        }
        node_params = {
            'consensus_only': bool(consensus_only),
            'header_size': header_size,
            'max_header_delay': 200,
            'gc_depth': 50,
            'sync_retry_delay': 10_000,
            'sync_retry_nodes': min(3, nodes - 1),
            'batch_size': batch_requests * tx_size,  # bytes == 4096 txs
            'tx_size': tx_size,
            'max_batch_delay': 1000,  # ms, matches ISS BATCH_DELAY
            'leaders_per_round': (2 * nodes + 2) // 3,
            # Seed value; CloudLabBench syncs each matrix propose_rate.
            'propose_rate': propose_rate_list[0],
            'f_num': f_num,
        }
        _cloudlab_bench()(ctx).run(
            bench_params,
            node_params,
            bool(debug),
            bool(consensus_only),
            update=bool(update),
            propose_rates=propose_rate_list,
        )
    except EnvironmentSetupError as error:
        Print.error(BenchError(
            'CloudLab local controller is not initialized',
            error,
        ))
    except (TypeError, ValueError) as error:
        Print.error(BenchError(
            'Invalid CloudLab benchmark parameters',
            error,
        ))
    except BenchError as error:
        Print.error(error)


@task
def cloudlab_kill(ctx):
    ''' Stop execution on all CloudLab nodes '''
    try:
        _cloudlab_bench()(ctx).kill()
    except BenchError as error:
        Print.error(error)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    from benchmark.plot import Ploter, PlotError

    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [2_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    try:
        _gcp_bench()(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))

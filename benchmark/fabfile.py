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
def local(ctx, debug=True, consensus_only=True, header_size=512):
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
        'propose_rate': 0.8,  # rate of proposing a header
        'f_num': 3,
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
def remote(ctx, burst = 50, debug=False, consensus_only=False, header_size=512):
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
        'propose_rate': 1,  # rate of proposing a header
        'f_num': 3,
    }
    try:
        _gcp_bench()(ctx).run(
            bench_params,
            node_params,
            debug,
            consensus_only,
        )
    except BenchError as e:
        Print.error(e)


@task
def cloudlab_install(ctx, max_workers=8):
    ''' Initialize CloudLab and install Angelfish on all CloudLab nodes '''
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


@task
def cloudlab_remote(
    ctx,
    nodes=4,
    rate=80000,
    burst=50,
    duration=60,
    runs=1,
    debug=False,
    consensus_only=False,
    header_size=512,
):
    ''' Run the remote benchmark workflow entirely on CloudLab '''
    from environment_setup import (
        EnvironmentSetupError,
        setup_local_environment,
    )

    try:
        setup_local_environment(check_only=True)

        nodes = int(nodes)
        rate = int(rate)
        burst = int(burst)
        duration = int(duration)
        runs = int(runs)
        header_size = int(header_size)
        if nodes <= 1:
            raise ValueError('nodes must be greater than one')
        if rate <= 0:
            raise ValueError('rate must be greater than zero')
        if burst <= 0:
            raise ValueError('burst must be greater than zero')
        if duration <= 0:
            raise ValueError('duration must be greater than zero')
        if runs <= 0:
            raise ValueError('runs must be greater than zero')
        if header_size <= 0:
            raise ValueError('header-size must be greater than zero')

        f_num = (nodes - 1) // 3
        bench_params = {
            'faults': 0,
            'nodes': nodes,
            'workers': 1,
            'collocate': True,
            # Aggregate target input rate across all CloudLab clients.
            'rate': [rate],
            'tx_size': 512,
            'duration': duration,
            'runs': runs,
            'burst': [burst],
        }
        node_params = {
            'consensus_only': bool(consensus_only),
            'header_size': header_size * 10000,
            'max_header_delay': 200,
            'gc_depth': 50,
            'sync_retry_delay': 10_000,
            'sync_retry_nodes': min(3, nodes - 1),
            'batch_size': header_size,
            'tx_size': bench_params['tx_size'],
            'max_batch_delay': 200,
            'leaders_per_round': (2 * nodes + 2) // 3,
            'propose_rate': 0.8,
            'f_num': f_num,
        }
        _cloudlab_bench()(ctx).run(
            bench_params,
            node_params,
            bool(debug),
            bool(consensus_only),
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

#!/usr/bin/env python3
"""Prepare CloudLab nodes from the local control machine.

This script is intentionally run locally. It reads ``cloudlab_settings.json``
and executes the environment setup on the selected remote CloudLab nodes over
SSH.
"""

import argparse
import importlib
import os
import shlex
import shutil
import site
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from benchmark.cloudlab_settings import (
    CloudLabSettings,
    CloudLabSettingsError,
)


LOCAL_DEPENDENCIES = {
    'fabric': 'fabric>=2.6.0,<4.0.0',
    'asyncssh': 'asyncssh>=2.13.0,<3.0.0',
}

LOCAL_SYSTEM_COMMANDS = (
    'cc',
    'clang',
    'cmake',
    'curl',
    'git',
    'pkg-config',
)
LOCAL_RUST_COMMANDS = ('cargo', 'rustc')


LOCAL_SYSTEM_SETUP_SCRIPT = r'''
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
    echo "Only Debian/Ubuntu local controllers are currently supported" >&2
    exit 1
fi

sudo -n apt-get update
sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    curl \
    git \
    libclang-dev \
    libssl-dev \
    pkg-config
'''


LOCAL_RUST_SETUP_SCRIPT = r'''
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to install Rust on the local controller" >&2
    exit 1
fi

if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable
fi

source "$HOME/.cargo/env"
rustup toolchain install stable
rustup default stable
cargo --version
rustc --version
'''


REMOTE_SETUP_SCRIPT = r'''
set -euo pipefail

if ! command -v apt-get >/dev/null 2>&1; then
    echo "Only Debian/Ubuntu CloudLab images are currently supported" >&2
    exit 1
fi

sudo -n apt-get update
sudo -n env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    curl \
    git \
    iproute2 \
    libclang-dev \
    libssl-dev \
    pkg-config \
    python3-pip \
    tmux

if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable
fi

source "$HOME/.cargo/env"
rustup toolchain install stable
rustup default stable

rustc --version
cargo --version
clang --version | head -n 1
cmake --version | head -n 1
tmux -V

mkdir -p "$HOME/logs"
echo "environment setup complete"
'''


REMOTE_CHECK_SCRIPT = r'''
set -euo pipefail

if [ -f "$HOME/.cargo/env" ]; then
    source "$HOME/.cargo/env"
fi

for command in cargo rustc clang cmake git tmux; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "missing command: $command" >&2
        exit 1
    fi
done

rustc --version
cargo --version
clang --version | head -n 1
cmake --version | head -n 1
tmux -V
echo "environment check complete"
'''


class EnvironmentSetupError(Exception):
    pass


@dataclass
class HostResult:
    hostname: str
    success: bool
    stdout: str = ''
    stderr: str = ''
    error: str = ''


def _activate_local_cargo():
    """Expose a rustup installation to this Fabric process."""
    cargo_bin = Path.home() / '.cargo' / 'bin'
    if not cargo_bin.is_dir():
        return

    path_entries = os.environ.get('PATH', '').split(os.pathsep)
    cargo_path = str(cargo_bin)
    if cargo_path not in path_entries:
        os.environ['PATH'] = os.pathsep.join([cargo_path, *path_entries])


def setup_local_environment(check_only=False, dry_run=False):
    """Prepare Python and Rust on the local benchmark controller."""
    def available(module):
        if module in sys.modules:
            return True
        try:
            importlib.import_module(module)
            return True
        except (ImportError, ModuleNotFoundError):
            return False

    _activate_local_cargo()
    missing_python = [
        package
        for module, package in LOCAL_DEPENDENCIES.items()
        if not available(module)
    ]

    print(f'Local controller Python: {sys.executable}')
    if not missing_python:
        print('Local controller dependencies: OK')
    else:
        print(
            'Missing local dependencies: '
            f'{", ".join(missing_python)}'
        )
        if dry_run:
            print(
                'Would install the missing packages with this '
                'Python interpreter'
            )
        elif check_only:
            raise EnvironmentSetupError(
                'Missing local controller dependencies: '
                f'{", ".join(missing_python)}'
            )
        else:
            command = [
                sys.executable,
                '-m',
                'pip',
                'install',
                '--disable-pip-version-check',
            ]
            if sys.prefix == sys.base_prefix:
                command.append('--user')
            command.extend(missing_python)

            print(
                'Installing local dependencies: '
                f'{" ".join(missing_python)}'
            )
            try:
                subprocess.run(command, check=True)
            except (OSError, subprocess.CalledProcessError) as error:
                raise EnvironmentSetupError(
                    'Failed to install local controller dependencies'
                ) from error

            # pip may create the user site directory after Fabric starts.
            user_site = site.getusersitepackages()
            if Path(user_site).is_dir():
                site.addsitedir(user_site)
            importlib.invalidate_caches()
            still_missing = [
                module
                for module in LOCAL_DEPENDENCIES
                if not available(module)
            ]
            if still_missing:
                raise EnvironmentSetupError(
                    'Installed packages are not importable by '
                    f'{sys.executable}: {", ".join(still_missing)}'
                )
            print('Local controller dependencies installed')

    missing_system = [
        command
        for command in LOCAL_SYSTEM_COMMANDS
        if shutil.which(command) is None
    ]
    if missing_system:
        print(
            'Missing local system build tools: '
            f'{", ".join(missing_system)}'
        )
        if dry_run:
            print(
                'Would install the local system build dependencies '
                'with apt'
            )
        elif check_only:
            raise EnvironmentSetupError(
                'Missing local system build tools: '
                f'{", ".join(missing_system)}; '
                'run "fab cloudlab-install" first'
            )
        else:
            print(
                'Installing system build dependencies on the '
                'local controller'
            )
            try:
                subprocess.run(
                    ['bash', '-lc', LOCAL_SYSTEM_SETUP_SCRIPT],
                    check=True,
                )
            except (OSError, subprocess.CalledProcessError) as error:
                raise EnvironmentSetupError(
                    'Failed to install local system build dependencies'
                ) from error

            still_missing = [
                command
                for command in LOCAL_SYSTEM_COMMANDS
                if shutil.which(command) is None
            ]
            if still_missing:
                raise EnvironmentSetupError(
                    'Installed local system tools are not available: '
                    f'{", ".join(still_missing)}'
                )
    else:
        print('Local controller system build tools: OK')

    missing_rust = [
        command
        for command in LOCAL_RUST_COMMANDS
        if shutil.which(command) is None
    ]
    if not missing_rust:
        print(
            'Local controller Rust: '
            f'{shutil.which("cargo")}'
        )
        return

    print(f'Missing local Rust tools: {", ".join(missing_rust)}')
    if dry_run:
        print('Would install the stable Rust toolchain with rustup')
        return
    if check_only:
        raise EnvironmentSetupError(
            'Missing local Rust tools: '
            f'{", ".join(missing_rust)}; '
            'run "fab cloudlab-install" first'
        )

    print('Installing the stable Rust toolchain on the local controller')
    try:
        subprocess.run(
            ['bash', '-lc', LOCAL_RUST_SETUP_SCRIPT],
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise EnvironmentSetupError(
            'Failed to install Rust on the local controller'
        ) from error

    _activate_local_cargo()
    still_missing = [
        command
        for command in LOCAL_RUST_COMMANDS
        if shutil.which(command) is None
    ]
    if still_missing:
        raise EnvironmentSetupError(
            'Installed local build tools are not available to Fabric: '
            f'{", ".join(still_missing)}'
        )
    print('Local controller Rust installed')


def _connection_kwargs(settings):
    key_path = Path(settings.key_path).expanduser()
    if not key_path.is_file():
        raise EnvironmentSetupError(f'SSH private key not found: {key_path}')

    kwargs = {'key_filename': str(key_path)}
    password = (
        os.environ.get('SSH_KEY_PASSWORD')
        or settings.key_password
    )
    if password:
        kwargs['passphrase'] = password
    return kwargs


def _run_on_host(host, connect_kwargs, script):
    from fabric import Connection

    hostname = host['hostname']
    connection = None
    try:
        connection = Connection(
            hostname,
            user=host['username'],
            port=host['port'],
            connect_kwargs=connect_kwargs,
            connect_timeout=30,
        )
        result = connection.run(
            f'bash -lc {shlex.quote(script)}',
            hide=True,
            warn=True,
            pty=False,
        )
        return HostResult(
            hostname=hostname,
            success=result.ok,
            stdout=result.stdout,
            stderr=result.stderr,
        )
    except Exception as error:
        return HostResult(
            hostname=hostname,
            success=False,
            error=str(error),
        )
    finally:
        if connection is not None:
            connection.close()


def _select_hosts(settings, hostnames):
    if not hostnames:
        return settings.hosts

    requested = set(hostnames)
    selected = [
        host for host in settings.hosts
        if host['hostname'] in requested
    ]
    missing = requested - {host['hostname'] for host in selected}
    if missing:
        raise EnvironmentSetupError(
            f'Unknown CloudLab host(s): {", ".join(sorted(missing))}'
        )
    return selected


def setup_environment(
    settings_file='cloudlab_settings.json',
    hostnames=None,
    max_workers=8,
    dry_run=False,
    check_only=False,
):
    """Set up the local controller and remote CloudLab nodes."""
    setup_local_environment(
        check_only=check_only,
        dry_run=dry_run,
    )

    try:
        settings = CloudLabSettings.load(settings_file)
    except CloudLabSettingsError as error:
        raise EnvironmentSetupError(str(error)) from error
    hosts = _select_hosts(settings, hostnames)
    if not hosts:
        raise EnvironmentSetupError('No CloudLab hosts selected')

    script = REMOTE_CHECK_SCRIPT if check_only else REMOTE_SETUP_SCRIPT
    action = 'check' if check_only else 'setup'

    print(f'CloudLab environment {action}: {len(hosts)} node(s)')
    for host in hosts:
        print(
            f'  {host["username"]}@{host["hostname"]}:{host["port"]}'
        )

    if dry_run:
        print('\nRemote script:\n')
        print(script.strip())
        return []

    connect_kwargs = _connection_kwargs(settings)
    worker_count = max(1, min(int(max_workers), len(hosts)))
    results = []

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_host = {
            executor.submit(
                _run_on_host, host, connect_kwargs, script
            ): host
            for host in hosts
        }
        for future in as_completed(future_to_host):
            result = future.result()
            results.append(result)
            label = 'OK' if result.success else 'FAIL'
            print(f'[{label}] {result.hostname}')
            if result.stdout.strip():
                print(result.stdout.rstrip())
            if result.stderr.strip():
                print(result.stderr.rstrip())
            if result.error:
                print(result.error)

    failures = [result.hostname for result in results if not result.success]
    if failures:
        raise EnvironmentSetupError(
            f'Environment {action} failed on: {", ".join(failures)}'
        )

    print(
        f'CloudLab environment {action} completed on '
        f'{len(results)} node(s)'
    )
    return results


def _parse_args():
    default_settings = Path(__file__).with_name(
        'cloudlab_settings.json'
    )
    parser = argparse.ArgumentParser(
        description='Set up CloudLab nodes from the local machine',
    )
    parser.add_argument(
        '--settings',
        default=str(default_settings),
        help='Path to cloudlab_settings.json',
    )
    parser.add_argument(
        '--hosts',
        help='Comma-separated hostnames; defaults to all configured hosts',
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=8,
        help='Maximum number of nodes configured in parallel',
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Verify the environment without installing anything',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Print targets and remote commands without connecting',
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    hostnames = (
        [host.strip() for host in args.hosts.split(',') if host.strip()]
        if args.hosts
        else None
    )
    try:
        setup_environment(
            settings_file=args.settings,
            hostnames=hostnames,
            max_workers=args.max_workers,
            dry_run=args.dry_run,
            check_only=args.check,
        )
    except EnvironmentSetupError as error:
        print(f'ERROR: {error}')
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

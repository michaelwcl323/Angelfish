# Copyright(C) Facebook, Inc. and its affiliates.
"""
CloudLab Instance Manager

This module provides functionality to manage CloudLab nodes.
Unlike AWS, CloudLab nodes are pre-allocated and accessed via SSH.
"""

from collections import defaultdict
from benchmark.utils import Print, BenchError
from benchmark.cloudlab_settings import CloudLabSettings, CloudLabSettingsError


class CloudLabInstanceManager:
    """Manages CloudLab nodes via SSH connections"""

    def __init__(self, settings):
        assert isinstance(settings, CloudLabSettings)
        self.settings = settings

    @classmethod
    def make(cls, settings_file='cloudlab_settings.json'):
        """Create an instance manager from settings file"""
        try:
            return cls(CloudLabSettings.load(settings_file))
        except CloudLabSettingsError as e:
            raise BenchError('Failed to load CloudLab settings', e)

    def hosts(self, flat=False):
        """
        Get list of hosts.

        Args:
            flat: If True, return flat list. If False, return dict grouped by region.

        Returns:
            List of hostnames or dict of {region: [hostnames]}
        """
        if flat:
            return [host['hostname'] for host in self.settings.hosts]
        else:
            # Group by region
            hosts_by_region = defaultdict(list)
            for host in self.settings.hosts:
                region = host.get('region', 'default')
                hosts_by_region[region].append(host['hostname'])
            return dict(hosts_by_region)

    def get_host_info(self):
        """
        Get detailed host information including username and hostname.

        Returns:
            List of dicts with 'hostname', 'username', and 'region'
        """
        return self.settings.hosts

    def host(self, hostname):
        """Return the connection settings for a hostname."""
        for host in self.settings.hosts:
            if host['hostname'] == hostname:
                return host
        raise KeyError(f'Unknown CloudLab host: {hostname}')

    def print_info(self):
        """Print information about all available CloudLab nodes"""
        hosts = self.hosts()
        key = self.settings.key_path
        host_info = self.get_host_info()

        text = ''
        for region, hostnames in hosts.items():
            text += f'\n Region: {region.upper()}\n'
            for i, hostname in enumerate(hostnames):
                # Find username and port for this host
                username = 'root'  # default
                port = 22  # default
                for host in host_info:
                    if host['hostname'] == hostname:
                        username = host.get('username', 'root')
                        port = host.get('port', 22)
                        break

                new_line = '\n' if (i+1) % 6 == 0 else ''
                if port != 22:
                    text += f'{new_line} {i}\tssh -i {key} -p {port} {username}@{hostname}\n'
                else:
                    text += f'{new_line} {i}\tssh -i {key} {username}@{hostname}\n'

        print(
            '\n'
            '----------------------------------------------------------------\n'
            ' CLOUDLAB INFO:\n'
            '----------------------------------------------------------------\n'
            f' Available machines: {sum(len(x) for x in hosts.values())}\n'
            f'{text}'
            '----------------------------------------------------------------\n'
        )

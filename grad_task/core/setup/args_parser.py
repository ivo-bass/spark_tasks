import argparse
import yaml


class ArgsParser:
    """Holds ArgumentParser() instance, adds argument 'config' and parses run command"""

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('config', help='YAML config file')

    def parse_configurations(self):
        """
        Parses run command to get config file path, reads config file and returns dictionary
        :return: dict: configurations
        """
        args = self.parser.parse_args()
        with open(args.config) as config_file:
            return yaml.safe_load(config_file)

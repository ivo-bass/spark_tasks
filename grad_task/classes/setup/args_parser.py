import argparse
import yaml


class ArgsParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('config', help='YAML config file')

    def parse_configurations(self):
        args = self.parser.parse_args()
        with open(args.config) as config_file:
            return yaml.safe_load(config_file)

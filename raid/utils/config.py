import os
import time
import yaml


class Config(object):
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self.load_config()
        assert self.config['disks_num'] == self.config['data_disks_num'] + \
            self.config['parity_disks_num']
        # assert self.config['block_size'] % 4 == 0
        # assert self.config['group_size'] % self.config["block_size"] == 0
        self.config["stripe_size"] = self.config['group_size'] * \
            self.config['data_disks_num']
        self.print_config()

    def print_config(self):
        print("Load config file: {}".format(self.config_file))
        print("disks_num: {}".format(self.config['disks_num']))
        print("data_disks_num: {}".format(self.config['data_disks_num']))
        print("parity_disks_num: {}".format(self.config['parity_disks_num']))
        print("group_size: {}".format(self.config['group_size']))

    def load_config(self):
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def get_config(self):
        return self.config


if __name__ == '__main__':
    config = Config('raid6_config.yaml')
    # config.print_config()
    # print(config.get_config())

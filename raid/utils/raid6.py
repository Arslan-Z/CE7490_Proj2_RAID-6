import sys
import os
import numpy as np
import math
import time
import shutil

from raid.utils.galois_field import GaloisField
from raid.utils.config import Config
from raid.utils.disk import Disk
from raid.utils.file import File
from raid.utils.utils import read_data, write_data

class RAID6(object):
    def __init__(self, config):
        self.config = config
        self.galois_field = GaloisField(config)
        self.organize_disks(config)
        
    def organize_disks(self, config):
        try:
            shutil.rmtree(config['data_dir'])
        except:
            pass
        self.data_disks, self.data_disks_id_list = self.build_data_disks(config)
        self.parity_disks, self.parity_disks_id_list = self.build_parity_disks(config)

    def build_data_disks(self, config):
        data_disks_id_list = list(range(config['data_disks_num']))
        data_disks = [Disk(i, config['data_dir'], config["stripe_size"], type="data") for i in data_disks_id_list]
        return data_disks, data_disks_id_list

    def build_parity_disks(self, config):
        parity_disks_id_list = list(range(
            config['data_disks_num'], config['data_disks_num'] + config['parity_disks_num']))
        parity_disks = [Disk(i, config['data_dir'], config["stripe_size"], type="parity") for i in parity_disks_id_list]
        return parity_disks, parity_disks_id_list

    def distribute_data(self, filename):
        data = self.read_data(filename)
        self.data_length = len(data)
        pass

    def read_disks(self, config):
        data = []
        for i in range(config['data_disks_num']):
            with open(os.path.join(config['data_dir'], "disk_{}".format(i)), 'rb') as f:
                data.append(list(f.read()))
        data = data[:]

    def write_to_disk(self, data):
        pass

    def caculate_parity(self, data):
        return self.galois_field.matmul(self.galois_field.vender_mat, data)

    def corrupt_disk(self, corrupted_disks_list):
        for i in corrupted_disks_list:
            pass
    def rebuild_data(self, corrupted_disks_list):
        pass

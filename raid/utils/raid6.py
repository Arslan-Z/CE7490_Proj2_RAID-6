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
    
    
    ## TODO: below are the functions that need to be implemented
    # def split_data_blocks_into_disks(self, data_blocks):
    #     block_to_disk_map = [None for _ in range(len(data_blocks))]
    #     for i, block_i in emumerate(data_blocks):
            

    def read_disks(self, config):
        data = []
        for i in range(config['data_disks_num']):
            with open(os.path.join(config['data_dir'], "disk_{}".format(i)), 'rb') as f:
                data.append(list(f.read()))
        data = data[:]
        pass
    

    def write_to_disk(self, data_blocks):
        stripe_num = len(data_blocks)
        data_blocks = np.asarray(data_blocks, dtype=int)
        data_blocks = data_blocks.reshape(self.config['disks_num'], self.config['chunk_size']*stripe_num)
        parity_blocks = self.caculate_parity(data_blocks)
        data_parity_blocks = np.concatenate((data_blocks, parity_blocks), axis=0)
        for i in range(self.config['disks_num']):
            data =  bytes(data_parity_blocks[i, :].tolist())
            write_data(os.path.join(self.config['data_dir'], "disk_{}".format(i)), data)

    def caculate_parity(self, data):
        return self.galois_field.matmul(self.galois_field.vender_mat, data)
        

    def corrupt_disk(self, corrupted_disks_list):
        for i in corrupted_disks_list:
        pass
        
    def rebuild_data(self, corrupted_disks_list):
        pass

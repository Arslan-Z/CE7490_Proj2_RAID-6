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
from raid.utils.utils import read_data, write_data, remove_data


class RAID6(object):
    def __init__(self, config):
        self.config = config
        self.galois_field = GaloisField(config)
        self.organize_disks(config)

    def build_data_disks(self, config):
        data_disks_id_list = list(range(config['data_disks_num']))
        data_disks = [Disk(i, config['data_dir'], config["stripe_size"],
                           type="data") for i in data_disks_id_list]
        return data_disks, data_disks_id_list

    def build_parity_disks(self, config):
        parity_disks_id_list = list(range(
            config['data_disks_num'], config['data_disks_num'] + config['parity_disks_num']))
        parity_disks = [Disk(i, config['data_dir'], config["stripe_size"],
                             type="parity") for i in parity_disks_id_list]
        return parity_disks, parity_disks_id_list

    def organize_disks(self, config):
        try:
            shutil.rmtree(config['data_dir'])
        except:
            pass
        self.data_disks, self.data_disks_id_list = self.build_data_disks(
            config)
        self.parity_disks, self.parity_disks_id_list = self.build_parity_disks(
            config)

    def set_content_size(self, content_size):
        self.content_size = content_size

    def read_from_disks(self, config):
        data = []
        all_disks = self.data_disks + self.parity_disks
        for i in range(config['data_disks_num']):
            disk = all_disks[i]
            file_content = disk.read_from_disk()
            # file_content=read_data(os.path.join(os.path.join(config['data_dir'], "disk_{}".format(i)), "disk_{}".format(i)))
            data += list(file_content)
        data = data[:self.content_size]
        return data

    def write_to_disk(self, data_blocks):
        stripe_num = len(data_blocks)

        data_blocks = np.asarray(data_blocks, dtype=int)
        data_blocks = data_blocks.reshape(
            self.config['data_disks_num'], self.config['chunk_size']*stripe_num)

        parity_blocks = self.caculate_parity(data_blocks)
        data_and_parity = np.concatenate((data_blocks, parity_blocks), axis=0)

        for disk, data in zip(self.data_disks + self.parity_disks, data_and_parity.tolist()):
            disk.write_to_disk(bytes(data))
        print("Write data disk and parity disk done")

    def caculate_parity(self, data):
        return self.galois_field.matmul(self.galois_field.vender_mat, data)

    def corrupt_disk(self, corrupted_disks_list):
        for i in corrupted_disks_list:
            remove_data(os.path.join(
                self.config['data_dir'], "disk_{}".format(i)))
            print("Corrupt disk {}".format(i))

    def recover_disk(self, corrupted_disks_list):
        print("Try to recover disks {}".format(corrupted_disks_list))
        assert len(corrupted_disks_list) <= self.config['parity_disks_num']

        healthy_data_disk_id_list = [
            i for i in self.data_disks_id_list if i not in corrupted_disks_list]
        healthy_parity_disk_id_list = [
            i for i in self.parity_disks_id_list if i not in corrupted_disks_list]

        all_disks = self.data_disks + self.parity_disks

        healthy_data = [all_disks[i].read_from_disk()
                        for i in healthy_data_disk_id_list]

        healthy_parity = [all_disks[i].read_from_disk()
                          for i in healthy_parity_disk_id_list]

        # healthy_data = [read_data(os.path.join(os.path.join(self.config['data_dir'], "disk_{}".format(
        #     i)), "disk_{}".format(i))) for i in healthy_data_disks]
        # healthy_parity = [read_data(os.path.join(os.path.join(self.config['data_dir'], "disk_{}".format(
        #     i)), "disk_{}".format(i))) for i in healthy_parity_disks]

        # matrix A concatenated by n x n identity matrix and vandermond matrix
        mat_A = np.concatenate([np.eye(
            self.config['data_disks_num'], dtype=int), self.galois_field.vender_mat], axis=0)
        mat_A_delete = np.delete(mat_A, obj=corrupted_disks_list, axis=0)

        # matrix E concatenated vector by byte in data disks and checksums
        mat_E_delete = np.concatenate(
            [np.asarray(healthy_data), np.asarray(healthy_parity)], axis=0)
        mat_D = self.galois_field.matmul(
            self.galois_field.inv(mat_A_delete), mat_E_delete)
        mat_C = self.galois_field.matmul(self.galois_field.vender_mat, mat_D)
        mat_E = np.concatenate([mat_D, mat_C], axis=0)
        all_disk = self.data_disks + self.parity_disks
        corrupted_disk = [
            disk for disk in all_disk if disk.disk_id in corrupted_disks_list]
        [disk.create_disk_folders(os.path.join(self.config['data_dir'], "disk_{}".format(
            disk.disk_id))) for disk in corrupted_disk]

        for i, disk in zip(corrupted_disks_list, corrupted_disk):
            disk.write_to_disk(bytes(mat_E[i, :].tolist()))
            print("Recover disk {}".format(i))

        # [write_data(os.path.join(os.path.join(self.config['data_dir'], "disk_{}".format(
        #     i)), "disk_{}".format(i)), bytes(mat_E[i, :].tolist())) for i in corrupted_disks_list]

        print("Recover data done!")

        # TODO: below are the functions that need to be implemented
        def check_corruption(self):
            pass

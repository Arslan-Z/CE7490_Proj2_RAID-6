import os
import numpy as np
import shutil

from .galois_field import GaloisField
from .disk import Disk
from .utils import read_data, write_data, remove_data


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
            shutil.rmtree(os.path.join(config['data_dir'], "disks"))
        except:
            pass
        self.data_disks, self.data_disks_id_list = self.build_data_disks(
            config)
        self.parity_disks, self.parity_disks_id_list = self.build_parity_disks(
            config)
        self.all_disks = self.data_disks + self.parity_disks
        
    def set_content_size(self, content_size):
        self.content_size = content_size

    def set_total_stripe(self, total_stripe):
        self.stripe_num = total_stripe

    def read_from_disks(self, config, corrupted_disks_list=[]):
        data = []

        for disk in self.all_disks:
            
            if disk.disk_id not in corrupted_disks_list:
                try:
                    file_content = disk.read_from_disk()
                    # file_content=read_data(os.path.join(os.path.join(config['data_dir'], "disk_{}".format(i)), "disk_{}".format(i)))
                    data += list(file_content)
                except:
                    print("Read data disk {} failed, dir not exist".format(disk.disk_id))
        # print("Len of data is {}".format(len(data)))
        # data = np.asarray(data).reshape(self.config['data_disks_num'], -1)
        # data = self.shift_data_group(data, 1)
        
        return data
    
    def get_content(self, data):
        return data[:self.content_size]
    
    def shift_data_group(self, data_array, direction):
        """_summary_: shift data group to left or right
        Args:
            data_array (np array): _description_
            direction (int): shift direction, -1 for left, 1 for right
        Returns:
            np array: data array after shift
        """
        for i in range(data_array.shape[0]):
            data_array[i] = np.roll(data_array[i], i*direction, axis=0)
        return data_array
        
    def write_to_disk(self, data_blocks):
        # print("data_blocks : {}".format(data_blocks))
        # print("stripe_num : {}".format(self.stripe_num))
        # data_blocks = np.asarray(data_blocks, dtype=int)
        data_blocks = np.asarray(data_blocks)
        data_blocks = data_blocks.reshape(
            self.config['data_disks_num'], self.config['chunk_size']*self.stripe_num)

        parity_blocks = self.caculate_parity(data_blocks)
        self.parity = parity_blocks
        data_and_parity = np.concatenate((data_blocks, parity_blocks), axis=0)

        # data_and_parity = self.shift_data_group(data_and_parity, -1)
        
        for disk, data in zip(self.all_disks, data_and_parity):
            disk.write_to_disk(data.tolist())
        print("Write data disk and parity disk done")

    def caculate_parity(self, data):
        return self.galois_field.matmul(self.galois_field.vender_mat, data)

    def corrupt_disk(self, corrupted_disks_list):
        for i in corrupted_disks_list:
            remove_data(os.path.join(
                self.config['data_dir']+"disks/", "disk_{}".format(i)))
            print("Corrupt disk {}".format(i))

    def recover_disk(self, corrupted_disks_list):
        print("Try to recover disks {}".format(corrupted_disks_list))
        assert len(corrupted_disks_list) <= self.config['parity_disks_num']
        
        healthy_data = self.read_from_disks(self.config, corrupted_disks_list)
        healthy_data = np.asarray(healthy_data).reshape(
            self.config['disks_num']-len(corrupted_disks_list), -1)

        # matrix I concatenated by n x n identity matrix and vandermond matrix
        mat_I_V = np.concatenate([np.eye(
            self.config['data_disks_num'], dtype=int), self.galois_field.vender_mat], axis=0)
        mat_I_V_delete = np.delete(mat_I_V, obj=corrupted_disks_list, axis=0)
        
        mat_D_P_delete = healthy_data
        
        mat_D = self.galois_field.matmul(
            self.galois_field.inv(mat_I_V_delete), mat_D_P_delete)
        
        mat_P = self.galois_field.matmul(self.galois_field.vender_mat, mat_D)
        
        mat_D_P = np.concatenate([mat_D, mat_P], axis=0)
        
        
        corrupted_disk = [
            disk for disk in self.all_disks if disk.disk_id in corrupted_disks_list]
        [disk.create_disk_folders() for disk in corrupted_disk]
        
        # mat_E = self.shift_data_group(mat_E, -1)
        
        for i, disk in zip(corrupted_disks_list, corrupted_disk):
            disk.write_to_disk(bytes(mat_D_P[i, :].tolist()))
            print("Recover disk {}".format(i))

        # [write_data(os.path.join(os.path.join(self.config['data_dir'], "disk_{}".format(
        #     i)), "disk_{}".format(i)), bytes(mat_E[i, :].tolist())) for i in corrupted_disks_list]

        print("Recover data done!")

    def check_corruption(self):
        data = self.read_from_disks(self.config)
        data_content = self.get_content(data)
        data_content = np.asarray(data_content, dtype=int)
        # print(data_content.shape)
        data_content = data_content.reshape(
            self.config['data_disks_num'], -1)
        # print(data_content.shape)
        # content_size = len(data_content)
        # data_blocks = []
        # if content_size % self.config["stripe_size"] != 0:
        #     data_content += [0 for _ in range(self.config["stripe_size"] -
        #                                       content_size % self.config["stripe_size"])]
        # data_blocks = [data_content[i:i+self.config["stripe_size"]]
        #                for i in range(0, len(data_content), self.config["stripe_size"])]

        # data_content = np.asarray(data_content, dtype=int)
        # data_content = data_content.reshape(
        #     self.config['data_disks_num'], self.config['chunk_size']*self.stripe_num)
        parity = self.caculate_parity(
            data_content[0:self.config['data_disks_num']])
        is_corrupted = np.bitwise_xor(np.array(self.parity), np.array(parity))
        if np.count_nonzero(is_corrupted) == 0:
            print("No corrupted disks")
        else:
            print("Exist corrupted data!")

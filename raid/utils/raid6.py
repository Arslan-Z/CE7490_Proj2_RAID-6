import os
from os.path import join
import numpy as np
import shutil

from .galois_field import GaloisField
from .disk import Disk
from .utils import remove_data, split_data, ROOT_DIR

timer = Timer()
class RAID6(object):
    def __init__(self, config):
        self.config = config
        self.m = config['parity_disks_num']
        self.k = config['data_disks_num']
        self.n = self.k+self.m
        self.galois_field = GaloisField(config)
        self.DISK_ROOT = join(ROOT_DIR, config['data_dir'], "disks")
        self.__organize_disks()

    def __build_disks(self):
        disks_id_list = list(range(self.n))
        self.all_disks = [Disk(i, self.DISK_ROOT) for i in disks_id_list]

    def __organize_disks(self):
        try:
            shutil.rmtree(self.DISK_ROOT)
        except:
            pass
        self.__build_disks()

    def __caculate_parity(self, data):
        return self.galois_field.matmul_3d(self.galois_field.Vmat, data)

    def get_content(self, data):
        return data[:self.content_size]

    def read_from_disks(self, corrupted_disks_list=[]):
        data = []
        
        for disk in self.all_disks:

            if disk.disk_id not in corrupted_disks_list:
                try:
                    file_content = disk.read_from_disk()
                    # file_content=read_data(os.path.join(os.path.join(config['data_dir'], "disk_{}".format(i)), "disk_{}".format(i)))
                    data += list(file_content)
                except:
                    print("Read data disk {} failed, dir not exist".format(
                        disk.disk_id))
        # print("Len of data is {}".format(len(data)))
        # data = np.asarray(data).reshape(self.k, -1)

        return data

    def distribute_to_disks(self, data):
        """Split data first then distribute data to disks.

        Args:
            data (list): data to be distributed to disks.
        """
        data_groups, self.content_size, self.stripe_num = split_data(
            self.config['stripe_size'], data)
        data_groups = np.asarray(data_groups)
        data_groups = data_groups.reshape(self.stripe_num, self.k, -1
                                          )

        parity_blocks = self.__caculate_parity(data_groups)
        self.parity = parity_blocks
        data_and_parity = np.concatenate((data_groups, parity_blocks), axis=1)

        for i in range(len(data_and_parity)):
            for j in range(self.n):
                k = (j + self.n-i) % self.n
                if j < self.k:
                    group_name = f"{i:02d}_{j:02d}_d"
                else:
                    group_name = f"{i:02d}_{(j-self.k):02d}_p"

                self.all_disks[k].write_to_disk(
                    group_name, data_and_parity[i][j])

        print("Write data disk and parity disk done")

    def remove_disks(self, corrupted_disks_list):
        for i in corrupted_disks_list:
            remove_data(os.path.join(
                self.config['data_dir']+"disks/", "disk_{}".format(i)))
            print("Corrupt disk {}".format(i))

    def recover_disks(self, corrupted_disks_list):
        print("Try to recover disks {}".format(corrupted_disks_list))
        assert len(
            corrupted_disks_list) <= self.config['parity_disks_num'], "Too many corrupted disks."

        healthy_data = self.read_from_disks(self.config, corrupted_disks_list)
        healthy_data = np.asarray(healthy_data).reshape(
            self.config['disks_num']-len(corrupted_disks_list), -1)

        # matrix I concatenated by n x n identity matrix and vandermond matrix
        mat_I_V = np.concatenate([np.eye(
            self.k, dtype=int), self.galois_field.Vmat], axis=0)
        mat_I_V_delete = np.delete(mat_I_V, obj=corrupted_disks_list, axis=0)

        mat_D_P_delete = healthy_data

        mat_D = self.galois_field.matmul(
            self.galois_field.inv(mat_I_V_delete), mat_D_P_delete)

        mat_P = self.galois_field.matmul(self.galois_field.Vmat, mat_D)

        mat_D_P = np.concatenate([mat_D, mat_P], axis=0)

        corrupted_disk = [
            disk for disk in self.all_disks if disk.disk_id in corrupted_disks_list]
        [disk.create_disk_folders() for disk in corrupted_disk]

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
        data_content = data_content.reshape(
            self.k, -1)
        parity = self.__caculate_parity(
            data_content[0:self.k])
        is_corrupted = np.bitwise_xor(np.array(self.parity), np.array(parity))
        if np.count_nonzero(is_corrupted) == 0:
            print("No corrupted disks")
        else:
            print("Exist corrupted data!")

    def detect_failed_disks_id(self):
        failed_disks_id = list(range(self.n))
        for _, dirs,_ in os.walk(self.DISK_ROOT):
            for dir_name in dirs:
               failed_disks_id.remove(int(dir_name.split("_")[-1]))
        return failed_disks_id
        
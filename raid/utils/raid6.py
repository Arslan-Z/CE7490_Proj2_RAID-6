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

    def set_content_size(self, content_size):
        self.content_size = content_size

    def set_total_stripe(self, total_stripe):
        self.stripe_num = total_stripe

    def read_from_disks(self, config):
        data = []
        all_disks = self.data_disks + self.parity_disks
        for i in range(config['data_disks_num']):
            disk = all_disks[i]
            try:
                file_content = disk.read_from_disk()
                # file_content=read_data(os.path.join(os.path.join(config['data_dir'], "disk_{}".format(i)), "disk_{}".format(i)))
                data += list(file_content)
            except:
                print("Read data disk {} failed, dir not exist".format(i))
        # print("Len of data is {}".format(len(data)))
        data = data[:self.content_size]
        return data

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

        for disk, data in zip(self.data_disks + self.parity_disks, data_and_parity):
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
        [disk.create_disk_folders() for disk in corrupted_disk]

        for i, disk in zip(corrupted_disks_list, corrupted_disk):
            disk.write_to_disk(bytes(mat_E[i, :].tolist()))
            print("Recover disk {}".format(i))

        # [write_data(os.path.join(os.path.join(self.config['data_dir'], "disk_{}".format(
        #     i)), "disk_{}".format(i)), bytes(mat_E[i, :].tolist())) for i in corrupted_disks_list]

        print("Recover data done!")

    def check_corruption(self):
        data_content = self.read_from_disks(self.config)
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

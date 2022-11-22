import os
from os.path import join
import numpy as np
import shutil

from numpy.core.shape_base import block
from .utils import read_data, write_data, remove_data


class Disk(object):
    def __init__(self, disk_id, DISK_ROOT):
        self.disk_id = disk_id
        self.disk_dir = os.path.join(DISK_ROOT, f'disk_{self.disk_id}')

        self.create_disk_folders()

    def get_disk_id(self):
        return self.disk_id

    def read_from_disk(self, mode='rb'):
        disk_groups_dict = {}
        for file_name in os.listdir(self.disk_dir):
            disk_groups_dict[file_name] = read_data(
                join(self.disk_dir, file_name),mode)
        return disk_groups_dict

    def write_to_disk(self, group_name, group, mode='wb'):
        if not os.path.exists(self.disk_dir):
            os.makedirs(self.disk_dir)
        write_data(os.path.join(self.disk_dir, group_name), group, mode)

    def create_disk_folders(self):
        disk_dir = self.disk_dir
        # if path dont exist
        if not os.path.exists(disk_dir):
            os.makedirs(disk_dir)
        else:  # if exists alr delete
            shutil.rmtree(disk_dir)
            os.makedirs(disk_dir)

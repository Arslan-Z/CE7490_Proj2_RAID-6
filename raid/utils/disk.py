import os
import numpy as np
import logging
import shutil

from numpy.core.shape_base import block


class Disk(object):
    def __init__(self, disk_id, disk_dir, stripe_size, size=100, type='data'):
        self.disk_id = disk_id
        self.size = size
        self.type = type
        # if self.type != "data":
        #     self.override = False
        # else:
        #     self.override = True
        self.disk_dir = self.create_disk_folders(
            disk_dir=os.path.join(disk_dir, 'disk_{}'.format(self.disk_id)))
        self.data_blocks = None
        self.stripe_size = stripe_size

    def get_disk_id(self):
        return self.disk_id

    def read_from_disk(self, mode='rb'):
        with open(os.path.join(self.disk_dir, 'disk_{}'.format(self.disk_id)), mode) as f:
            return f.read()

    def write_to_disk(self, data, mode='wb'):
        with open(os.path.join(self.disk_dir, 'disk_{}'.format(self.disk_id)), mode) as f:
            f.write(bytes(data))
            logging.info('Write data into disk_{}'.format(self.disk_id))

    def get_data_blocks(self):
        if self.stripe_size == None:
            raise Exception('stripe_size must be set')

        data_content = list(self.read_from_disk())
        # print("data_content", data_content)
        content_size = len(data_content)

        # stripe_num = size_content // self.stripe_size + 1
        # total_stripe_size = stripe_num * self.stripe_size

        data_blocks = []
        if content_size % self.stripe_size != 0:
            data_content += [0 for _ in range(self.stripe_size -
                                              content_size % self.stripe_size)]

        data_blocks = [data_content[i:i+self.stripe_size]
                       for i in range(0, len(data_content), self.stripe_size)]
        return data_blocks, content_size

    def create_disk_folders(self, disk_dir):

        # if path dont exist
        if not os.path.exists(disk_dir):
            os.makedirs(disk_dir)
            logging.info('Disk {0} is created at {1}'.format(
                self.disk_id, str(disk_dir)))
        else:  # if exists alr delete
            shutil.rmtree(disk_dir)
            os.makedirs(disk_dir)
            # if self.override == True:
            #     shutil.rmtree(disk_dir)
            #     os.makedirs(disk_dir)
            # else:
            #     logging.info(
            #         'Disk {0} is already created'.format(str(disk_dir)))

        return disk_dir

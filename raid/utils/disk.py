import os
import numpy as np
import logging
import shutil

from numpy.core.shape_base import block

class Disk(object):
    def __init__(self, disk_dir, disk_id, size, stripe_size, type='data'):
        self.disk_id = disk_id
        self.size = size
        self.type = type
        if self.type == 'P' or self.type == 'Q':
            self.override = False
        else:
            self.override = True
        self.disk_dir = self.create_folders(
            disk_dir=os.path.join(disk_dir, 'disk_%d' % self.disk_id))
        self.data_blocks = None
        self.stripe_size = stripe_size

    def get_id(self):
        return self.disk_id

    def read(self):
        with open(os.path.join(self.disk_dir, 'disk_%d' % self.disk_id), 'r', encoding="utf-8") as f:
            return f.read()

    def write(self, data):
        with open(os.path.join(self.disk_dir, 'disk_%d' % self.disk_id), 'w', encoding="utf-8") as f:
            for i in data:
                f.write(i)
            logging.info('Write done at{0}'.format(self.disk_id))

    def get_data_block(self):

        if self.stripe_size == None:
            raise Exception('stripe_size must be set')

        data_content = self.read()
        size_content = len(data_content)
        data_blocks = []
        # if size of content is not multiple of stripe size
        # Padding remainder data with 0
        if size_content % self.stripe_size != 0:
            for _ in range(self.stripe_size - (size_content % self.stripe_size)):
                # data_content = str(data_content + 0
                print(data_content)

        assert size_content % self.stripe_size == 0
        # get blocks of data
        for i in range(0, size_content, self.stripe_size):
            end_of_block_idx = min(size_content, i+self.stripe_size)
            data_blocks.append(data_content[i:end_of_block_idx])
        # self.data_blocks = data_blocks
        # print(data_blocks)
        return data_blocks

    def create_folders(self, disk_dir):

        # if path dont exist
        if not os.path.exists(disk_dir):
            os.makedirs(disk_dir)
            logging.info('Disk {0} is created at {1}'.format(
                self.disk_id, str(disk_dir)))
        else:  # if exists alr delete
            if self.override == True:
                shutil.rmtree(disk_dir)
                os.makedirs(disk_dir)
            else:
                logging.info(
                    'Disk {0} is already created'.format(str(disk_dir)))

        return disk_dir

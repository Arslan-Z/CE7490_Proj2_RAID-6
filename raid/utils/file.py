import numpy as np
import string
import logging
import os

from .utils import str_to_bytes
class File(object):
    def __init__(self, file_id):
        self.data = None
        self.file_id = file_id

    def set_content(self, data):
        self.data = data

    def get_content(self):
        return np.array(self.data)

    # generate 'data_size' number of ascii characters
    def generate_random_data(self, data_size):
        ascii_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
                      'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
        # randomly selects # of characters from above
        data = np.random.choice(ascii_list, size=data_size)
        # print("data", data)
        # print("data type", type(data))
        # str = data.tostring()
        # print("str", str)
        # print("str type", type(str))
        self.data = data
        # logging.info('Generating string {0}'.format(str(self.data)))

    def write_to_disk(self, file_path, file_name):
        with open(os.path.join(file_path, file_name), 'w', encoding="utf-8") as f:
            for i in self.data:
                f.write(i)
            # logging.info('Write done at{0}'.format(file_name))


if __name__ == '__main__':
    file = File(1)
    file.generate_random_data(100)
    print(file.get_content())
    file.set_content(list("Hello NTU"))
    print(file.get_content())

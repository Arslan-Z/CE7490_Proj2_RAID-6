from os import pardir
from os.path import abspath, dirname, join
import shutil

ROOT_DIR = abspath(join(dirname(__file__), pardir, pardir))


def write_data(file_name, data, mode='wb'):
    with open(file_name, mode) as f:
        f.write(bytes(data))


def read_data(file_name, mode='rb'):
    with open(file_name, mode) as f:
        data = f.read()
    return list(data)


def remove_data(file_name):
    shutil.rmtree(file_name)


def str_to_list(string):
    # clean_string = string.split('\x00', 1)[0]
    clean_string = string.replace('\x00', '')
    return [word for word in clean_string]


def str_to_bytes(string):
    return bytes(string.strip(), 'utf-8')


def list_to_str(list):
    return ''.join(list)

def split_data(strip_size, data_content):
    """Split data into strips of size strip_size

    Args:
        strip_size (int): Size of each strip
        data_content (list): Data

    Returns:
        tuple(data_blocks, content_size, total_strip)
    """    
    assert strip_size, 'strip_size must be set'

    content_size = len(data_content)
    print("content_size", content_size)

    data_blocks = []
    if content_size % strip_size != 0:
        data_content += [0 for _ in range(strip_size -
                                            content_size % strip_size)]
    total_strip = len(data_content) // strip_size
    print("total_strip", total_strip)
    data_blocks = [data_content[i:i+strip_size]
                    for i in range(0, len(data_content), strip_size)]
    # print("data_blocks len", len(data_blocks))
    return data_blocks, content_size, total_strip
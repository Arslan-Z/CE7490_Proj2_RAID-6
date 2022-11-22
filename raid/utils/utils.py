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

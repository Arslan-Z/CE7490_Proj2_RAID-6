def write_data(file_name, data):
    with open(write_dir, 'wb') as f:
        f.write(bytes(data))

def read_data(file_name):
    with open(file_name, 'rb') as f:
        data = f.read()
    return list(data)


def remove_data(file_name):
    os.remove(file_name)

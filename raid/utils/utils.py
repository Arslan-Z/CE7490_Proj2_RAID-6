def write_data():
    pass

def read_data(file):
    with open(file, 'rb') as f:
        data = f.read()
    return list(data)

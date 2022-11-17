import os
import sys
import numpy as np
import time
from raid.utils.config import Config
from raid.utils.disk import Disk
from raid.utils.file import File
from raid.utils.raid6 import RAID6
from raid.utils.galois_field import GaloisField
from raid.utils.config import Config
from raid.utils.utils import read_data, write_data

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CURRENT_PATH)
# PAR_PATH = os.path.abspath(os.path.join(CURRENT_PATH, os.pardir))
# sys.path.append(PAR_PATH)

class TestRaid6(object):
    def __init__(self, config):
        
        config['data_dir'] = os.path.join(CURRENT_PATH, config['data_dir'])
        config['test_dir'] = os.path.join(CURRENT_PATH, config['test_dir'])
        
        test_log_dir = self.build_test_log_dir(config)
        config["test_log_dir"] = test_log_dir
        
        self.config = config
        
        self.raid_controller = RAID6(config)
        
        self.test_pipeline(config)
        
        
    def test_pipeline(self, config):
        file = File(1)
        file.generate_random_data(1000)
        raw_data = file.get_content()
        
        logical_disk = Disk(-1, config['data_dir'], config["stripe_size"], type="data")
        
        logical_disk.write_to_disk(raw_data)
        data_blocks, content_size = logical_disk.get_data_blocks()
        
        self.raid_controller.set_content_size(content_size)
        
        self.raid_controller.write_to_disk(data_blocks)
        
        corrupted_disks_list = [0, 1]
        
        self.raid_controller.corrupt_disk(corrupted_disks_list)
        
        self.raid_controller.recover_disk(corrupted_disks_list)
        
        rebuild_data = self.raid_controller.read_from_disks(config)
        
        print("raw_data: ", raw_data)
        print("rebuild_data: ", rebuild_data)
        # write_data(os.path.join(config['data_dir'], "rebuild_data"), rebuild_data)
        
    def build_test_log_dir(self, config):
        test_log_dir = os.path.join(config['test_dir'], time.strftime('%Y-%m-%d-%H-%M-%S'))
        if not os.path.exists(test_log_dir):
            os.makedirs(test_log_dir)
        return test_log_dir
    
    
    
if __name__ == "__main__":
    config = Config("raid/configs/raid6_config.yaml")
    test_raid6 = TestRaid6(config.config)
        
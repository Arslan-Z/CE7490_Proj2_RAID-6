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

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CURRENT_PATH)
# PAR_PATH = os.path.abspath(os.path.join(CURRENT_PATH, os.pardir))
# sys.path.append(PAR_PATH)

from src.raid.raid_6 import RAID_6
from src.file import File
from src.disk import Disk
from src.util import Configuration, Logger

class TestRaid6(object):
    def __init__(self, config):
        self.config = config
        config['data_dir'] = os.path.join(CURRENT_PATH, config['data_dir'])
        config['test_dir'] = os.path.join(CURRENT_PATH, config['test_dir'])
        
        test_log_dir = self.build_test_log_dir(config)
        config["test_log_dir"] = test_log_dir
        self.raid_controller = RAID6(config)
        
        # file = File(1)
        # file.generate_random_data(1000)
        # raw_data = file.get_content()
        
        self.test_pipeline()
        
        
    def test_pipeline(self):
        file = File(1)
        file.generate_random_data(1000)
        raw_data = file.get_content()
        
        logical_disk = Disk(-1, self.config['data_dir'], self.config["stripe_size"], type="data")
        
        logical_disk.write_to_disk(raw_data)
        data_blocks = logical_disk.get_data_blocks()
        
        
        self.raid_controller.write_to_disk(data_blocks)
        
        self.raid_controller.corrupt_disk([0, 1, 2])
        
        self.raid_controller.rebuild_disk([0, 1, 2])
        
    def build_test_log_dir(self, config):
        test_log_dir = os.path.join(config['test_dir'], time.strftime('%Y-%m-%d-%H-%M-%S'))
        if not os.path.exists(test_log_dir):
            os.makedirs(test_log_dir)
        return test_log_dir
    
    
    
if __name__ == "__main__":
    config = Config("raid/configs/raid6_config.yaml")
    test_raid6 = TestRaid6(config)
        
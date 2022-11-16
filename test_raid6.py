import os
import numpy as np
import time
from raid.utils.config import Config
from raid.utils.disk import Disk
from raid.utils.file import File
from raid.utils.raid6 import RAID6
from raid.utils.galois_field import GaloisField
from raid.utils.config import Config

class TestRaid6(object):
    def __init__(self, config):
        self.config = config
        test_log_dir = self.build_test_log_dir(config)
        config["test_log_dir"] = test_log_dir
        self.raid_controller = RAID6(config)
        
        file = File(1)
        file.generate_random_data(1000)
        raw_data = file.get_content()
        self.test_pipeline()
        
        
    def test_pipeline(self):
        
        self.raid_controller.write_to_disk(raw_data)
        
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
        
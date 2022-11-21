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
from raid.utils.utils import read_data, write_data, str_to_list

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CURRENT_PATH)
# PAR_PATH = os.path.abspath(os.path.join(CURRENT_PATH, os.pardir))
# sys.path.append(PAR_PATH)


class TestRaid6(object):
    def __init__(self, config):
        self.print_spliter()
        print(" "*9+"Start the test pipeline!")
        config['data_dir'] = os.path.join(CURRENT_PATH, config['data_dir'])
        config['test_dir'] = os.path.join(CURRENT_PATH, config['test_dir'])

        test_log_dir = self.build_test_log_dir(config)
        config["test_log_dir"] = test_log_dir

        self.config = config

        self.raid_controller = RAID6(config)

        self.test_pipeline(config)

    def print_spliter(self):
        print("=============================================")

    def prepare_data(self, config):
        file = File(1)
        file.generate_random_data(config["random_data_size"])
        raw_data = file.get_content()
        print("raw_data: ", raw_data)
        disk = Disk(-1, config['data_dir'],
                    config["stripe_size"], type="data")
        disk.write_to_disk(raw_data)
        data_blocks, content_size, total_stripe = disk.get_data_blocks()
        return data_blocks, content_size, total_stripe

    def init_raid_controller(self, data_blocks, content_size, total_stripe):
        self.raid_controller.set_content_size(content_size)
        self.raid_controller.set_total_stripe(total_stripe)
        self.raid_controller.write_to_disk(data_blocks)

    def test_corrupt_disk(self, corrupted_disks_list):
        self.raid_controller.corrupt_disk(corrupted_disks_list)

    def test_recovery_disk(self, config, corrupted_disks_list):
        self.raid_controller.recover_disk(corrupted_disks_list)

        rebuild_data = self.raid_controller.read_from_disks(config)
        # print("rebuild_data: ", rebuild_data)
        # print("raw_data: ", raw_data)
        # rebuild_data_str = [chr(i) for i in rebuild_data]
        rebuild_data_str = "".join([chr(i) for i in rebuild_data])
        rebuild_data = str_to_list(rebuild_data_str)

        # print("rebuild_data str: ", rebuild_data_str)
        # print("rebuild_data: ", rebuild_data)

        return rebuild_data
        # write_data(os.path.join(config['data_dir'], "rebuild_data"), rebuild_data)

    def manual_distort_data(self, disk_id, distort_loc):
        disk = self.raid_controller.data_disks[disk_id]
        original_data = disk.read_from_disk()
        # print("original_data: ", original_data)
        distorted_data = original_data
        distorted_data[distort_loc] = original_data[distort_loc]+1
        print("Manual distort data on disk_{} at location {}: ".format(
            disk_id, distort_loc))
        disk.write_to_disk(distorted_data)

    def test_corruption_detection(self, disk_id, distort_loc):
        print("Run corruption detection")
        self.raid_controller.check_corruption()
        print("Finish detection")
        self.manual_distort_data(disk_id=disk_id, distort_loc=distort_loc)
        print("Run corruption detection")
        self.raid_controller.check_corruption()
        print("Finish detection!")

    def test_pipeline(self, config):
        self.print_spliter()
        data_blocks, content_size, total_stripe = self.prepare_data(config)

        self.print_spliter()
        self.init_raid_controller(data_blocks, content_size, total_stripe)

        self.print_spliter()
        self.test_corruption_detection(disk_id=0, distort_loc=0)
        # detected_corrupted_disks = self.test_corrupted_disks_detection()
        self.print_spliter()
        corrupted_disks_list = [0, 1]
        self.test_corrupt_disk(corrupted_disks_list)

        self.print_spliter()
        rebuild_data = self.test_recovery_disk(
            config, corrupted_disks_list)
        self.print_spliter()

        print(" "*9+"Finish all test pipeline!")
        self.print_spliter()

        # self.raid_controller.recover_disk(corrupted_disks_list)

        # rebuild_data = self.raid_controller.read_from_disks(config)
        # # print("rebuild_data: ", rebuild_data)
        # # print("raw_data: ", raw_data)
        # rebuild_data_str = "".join([chr(i) for i in rebuild_data])
        # # rebuild_data_str = [chr(i) for i in rebuild_data]
        # print("rebuild_data str: ", rebuild_data_str)
        # rebuild_data = str_to_list(rebuild_data_str)

        # print("rebuild_data: ", rebuild_data)
        # # write_data(os.path.join(config['data_dir'], "rebuild_data"), rebuild_data)

    def build_test_log_dir(self, config):
        test_log_dir = os.path.join(
            config['test_dir'], time.strftime('%Y-%m-%d-%H-%M-%S'))
        if not os.path.exists(test_log_dir):
            os.makedirs(test_log_dir)
        return test_log_dir


if __name__ == "__main__":
    config = Config("raid/configs/raid6_config.yaml")
    test_raid6 = TestRaid6(config.config)

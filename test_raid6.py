import os
from os import makedirs
from os.path import exists
import time
from raid.utils import Config, Disk, File, RAID6, ROOT_DIR, remove_data, read_data


class TestRaid6(object):
    def __init__(self, config):
        self.print_spliter()
        print(" "*9+"Start the test pipeline!")
        config['data_dir'] = os.path.join(ROOT_DIR, config['data_dir'])
        config['test_dir'] = os.path.join(ROOT_DIR, config['test_dir'])

        test_log_dir = self.build_test_log_dir(config)
        config["test_log_dir"] = test_log_dir

        self.config = config

        self.raid_controller = RAID6(config)

        if config["mode"] == 1:
            self.test_pipeline(config, self.get_real_data)
        else:
            self.test_pipeline(config, self.get_synthetic_data)

    def print_spliter(self):
        print("=============================================")

    def get_synthetic_data(self, config):
        file = File(1)
        file.generate_random_data(config["random_data_size"])
        data = file.get_content()
        print("raw_data: ", data)
        return data

    def get_real_data(self, config):
        data = read_data(os.path.join(
            config['data_dir'], "real_data/")+config["real_file_name"])
        return data

    def rebuild_data(self, corrupted_disks_list):
        self.raid_controller.recover_disks(corrupted_disks_list)

        rebuild_data = self.raid_controller.read_from_disks()
        rebuild_data = self.raid_controller.get_content(rebuild_data)

        return rebuild_data

    def save_rebuid_data(self, config, rebuild_data):
        rebuilt_path = os.path.join(config['data_dir'], "rebuild_data")
        if exists(rebuilt_path):
            remove_data(rebuilt_path)
        makedirs(rebuilt_path)
        if self.config["mode"] == 0:
            # os.mkdir(os.path.join(config['data_dir'], "rebuild_data"))
            file_name = os.path.join(
                config['data_dir'], "rebuild_data/")+"rebuild_data"
            with open(file_name, mode="wb") as f:
                f.write(bytes(rebuild_data))

        elif self.config["mode"] == 1:
            file_name = os.path.join(
                config['data_dir'], "rebuild_data/")+"rebuild_"+config["real_file_name"]
            with open(file_name, mode="wb") as f:
                f.write(bytes(rebuild_data))
        print("Done writing rebuild data!")

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
        print("\n")
        self.manual_distort_data(disk_id=disk_id, distort_loc=distort_loc)
        print("\n")
        print("Run corruption detection")
        self.raid_controller.check_corruption()
        print("Finish detection!")

    def test_pipeline(self, config, get_data_func):
        self.print_spliter()
        data = get_data_func(config)

        self.print_spliter()
        self.raid_controller.distribute_to_disks(data)

        self.print_spliter()
        corrupted_disks_list = [0, 6]
        self.raid_controller.remove_disks(corrupted_disks_list)
        failed_disks_id = self.raid_controller.detect_failed_disks_id()
        self.print_spliter()
        data_rebuilt = self.rebuild_data(failed_disks_id)

        self.raid_controller.recover_disks(failed_disks_id)

        if config["is_save_rebuild"]:
            self.save_rebuid_data(config, data_rebuilt)
        self.print_spliter()

        print(" "*9+"Finish all test pipeline!")
        self.print_spliter()

    def build_test_log_dir(self, config):
        test_log_dir = os.path.join(
            config['test_dir'], time.strftime('%Y-%m-%d-%H-%M-%S'))
        if not os.path.exists(test_log_dir):
            os.makedirs(test_log_dir)
        return test_log_dir


if __name__ == "__main__":
    config = Config("raid/configs/raid6_config.yaml")
    test_raid6 = TestRaid6(config.config)

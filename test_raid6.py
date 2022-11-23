import os
from os import makedirs
from os.path import exists
import time
from raid.utils import Config, File, RAID6, ROOT_DIR, remove_data, read_data, write_data, str_to_list
class TestRaid6(object):
    def __init__(self, config, save_test_log=False):
        self.print_spliter()
        print(" "*5+"Start the test pipeline!")
        config['data_dir'] = os.path.join(ROOT_DIR, config['data_dir'])
        config['test_dir'] = os.path.join(ROOT_DIR, config['test_dir'])

        if save_test_log:
            test_log_dir = self.build_test_log_dir(config)
            config["test_log_dir"] = test_log_dir

        self.config = config

        self.raid_controller = RAID6(config)

        if config["mode"] == 1:
            self.test(config, self.get_real_data)
        else:
            self.test(config, self.get_synthetic_data)

    def print_spliter(self):
        print("=============================================")

    def build_test_log_dir(self, config):
        test_log_dir = os.path.join(
            config['test_dir'], time.strftime('%Y-%m-%d-%H-%M-%S'))
        if not os.path.exists(test_log_dir):
            os.makedirs(test_log_dir)
        return test_log_dir

    def get_synthetic_data(self, config):
        file = File(1)
        file.generate_random_data(config["random_data_size"])
        data = file.get_content()
        print("raw_data: ", data)
        data = data.tobytes()
        data = list(data)
        return data

    def get_real_data(self, config):
        data = read_data(os.path.join(
            config['data_dir'], "real_data/")+config["real_file_name"])
        return data

    def rebuild_data(self, corrupted_disks_list):
        self.raid_controller.recover_disks(corrupted_disks_list)
        rebuild_data = self.raid_controller.get_content()

        return rebuild_data

    def save_rebuid_data(self, config, rebuild_data):
        """Save the rebuilt file to a new folder

        Args:
            config (dict): The configuration dictionary.
            rebuild_data (list): Re-constructed data.
        """        
        rebuilt_path = os.path.join(config['data_dir'], "rebuild_data")
        if exists(rebuilt_path):
            remove_data(rebuilt_path)
        makedirs(rebuilt_path)
        if self.config["mode"] == 0:
            rebuild_data_str = "".join([chr(i) for i in rebuild_data])
            # print("rebuild_data: ", rebuild_data_str)
            rebuild_data = str_to_list(rebuild_data_str)
            # print("rebuild_data: ", rebuild_data)
            file_name = os.path.join(
                config['data_dir'], "rebuild_data/")+"rebuild_data.txt"
            with open(file_name, mode="w") as f:
                f.write(str(rebuild_data))

        elif self.config["mode"] == 1:
            file_name = os.path.join(
                config['data_dir'], "rebuild_data/")+"rebuild_"+config["real_file_name"]
            write_data(file_name, rebuild_data)
        print("Done saving rebuilt file!")

    def manual_distort_data(self, disk_id, distort_loc):
        """Manually distort data at a specific location on a disk.

        Args:
            disk_id (int): Disk ID.
            distort_loc (int): Location to distort data.
        """        
        disk = self.raid_controller.data_disks[disk_id]
        original_data = disk.read_from_disk()
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

    def test(self, config, get_data_func):
        """Test pipeline.

        Args:
            config (dict): Config.
            get_data_func (func): Function to get data.
        """        
        # Get data
        self.print_spliter()
        data = get_data_func(config)

        # Distribute data to distributed file system
        self.print_spliter()
        self.raid_controller.distribute_to_disks(data)

        # Randomly remove disks
        self.print_spliter()
        corrupted_disks_list = [0, 2]
        self.raid_controller.remove_disks(corrupted_disks_list)

        # Detect failed disks
        failed_disks_id = self.raid_controller.detect_failed_disks_id()
        self.print_spliter()

        # Rebuild failed disks
        data_rebuilt = self.rebuild_data(failed_disks_id)

        # Save rebuilt file.
        if config["is_save_rebuild"]:
            self.save_rebuid_data(config, data_rebuilt)
        self.print_spliter()

        print(" "*5+"Finish all test pipeline!")
        self.print_spliter()



if __name__ == "__main__":
    config = Config("raid/configs/raid6_config.yaml")
    test_raid6 = RAID6Test(config.config)

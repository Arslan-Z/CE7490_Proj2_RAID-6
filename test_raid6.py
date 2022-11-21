import os
import sys
import numpy as np
import time
import PIL.Image as Image
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

        if config["mode"] == 1:
            self.test_pipeline(config, self.prepare_real_data)
        else:
            self.test_pipeline(config, self.prepare_synthetic_data)

    def print_spliter(self):
        print("=============================================")

    def prepare_synthetic_data(self, config):
        file = File(1)
        file.generate_random_data(config["random_data_size"])
        raw_data = file.get_content()
        print("raw_data: ", raw_data)
        disk = Disk(-1, config['data_dir'],
                    config["stripe_size"], type="data")
        disk.write_to_disk(raw_data)
        data_blocks, content_size, total_stripe = disk.get_data_blocks(
            disk.read_from_disk())
        return data_blocks, content_size, total_stripe

    def prepare_real_data(self, config):
        disk = Disk(-2, config['data_dir'], config["stripe_size"], type="data")
        data = read_data(os.path.join(
            CURRENT_PATH, "real_data/")+config["real_file_name"])
        # # print("data type: ", type(data))
        # img = Image.open(os.path.join(
        #     CURRENT_PATH, "real_data/")+config["real_file_name"])
        # print("img.size: ", img.size)
        # self.img_size = img.size
        # self.img_mode = img.mode
        # # data = np.asarray(img, dtype=np.uint8).flatten().tolist()
        
        # # pixels = list(img.getdata())
        # pixels_array = np.array(img)
        # # print("pixels[0]: ", pixels_array[0])
        # # print("pixels[0] type: ", type(pixels_array[0]))
        # # # print("pixels: ", pixels)
        # # print("len(pixels): ", len(pixels_array))
        # # # width, height = img.size
        # # # pixels = [pixels[i * width:(i + 1) * width] for i in xrange(height)]
        # data = pixels_array.flatten().tolist()

        # data = data.flatten()
        # data = list(img)
        # print("data type:", type(data))
        # print("real data: ", data)
        data_blocks, content_size, total_stripe = disk.get_data_blocks(data)
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
        print("\n")
        self.manual_distort_data(disk_id=disk_id, distort_loc=distort_loc)
        print("\n")
        print("Run corruption detection")
        self.raid_controller.check_corruption()
        print("Finish detection!")

    def test_pipeline(self, config, data_generation):
        self.print_spliter()
        data_blocks, content_size, total_stripe = data_generation(
            config)

        self.print_spliter()
        self.init_raid_controller(data_blocks, content_size, total_stripe)

        # self.print_spliter()
        # self.test_corruption_detection(disk_id=0, distort_loc=0)
        # # detected_corrupted_disks = self.test_corrupted_disks_detection()

        self.print_spliter()
        corrupted_disks_list = [0, 1]
        self.test_corrupt_disk(corrupted_disks_list)

        self.print_spliter()
        rebuild_data = self.test_recovery_disk(
            config, corrupted_disks_list)
        self.print_spliter()

        print(" "*9+"Finish all test pipeline!")
        self.print_spliter()

        if self.config["mode"] == 0:
            # rebuild_data_str = "".join([chr(i) for i in rebuild_data])
            # rebuild_data = str_to_list(rebuild_data_str)
            # print("rebuild_data str: ", rebuild_data_str)
            # print("rebuild_data: ", rebuild_data)
            os.mkdir(os.path.join(config['data_dir'], "rebuild_data"))
            file_name = os.path.join(
                config['data_dir'], "rebuild_data/")+"rebuild_data"
            with open(file_name, mode="wb") as f:
                # f.write(str(rebuild_data))
                f.write(bytes(rebuild_data))
                # f.write(rebuild_data_str)
                

        elif self.config["mode"] == 1:
            file_name = os.path.join(
                CURRENT_PATH, "real_data/")+"rebuild_"+config["real_file_name"]
            with open(file_name, mode="wb") as f:
                f.write(bytes(rebuild_data))
            # # print("rebuild_data type: ", type(rebuild_data))
            # # w, h = self.img_size
            # rebuild_data = np.array(rebuild_data)
            # print("rebuild_data shape: ", rebuild_data.shape)

            # # print("rebuild_data: ", rebuild_data)

            # # write_data(os.path.join(
            # #     CURRENT_PATH, "real_data/")+"rebuild_"+config["real_file_name"], rebuild_data)
            # file_name = os.path.join(
            #     CURRENT_PATH, "real_data/")+"rebuild_"+config["real_file_name"]
            # rebuild_img = Image.fromarray(np.asarray(rebuild_data, dtype=np.uint8), mode=self.img_mode)
            # rebuild_img.save(file_name)
            # # with open(file_name, mode="wb") as f:
            # #     # f.write(str(rebuild_data))
            # #     # f.write(rebuild_data)
            # #     f.write(bytes(rebuild_data))

        print("Done writing rebuild data!")
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

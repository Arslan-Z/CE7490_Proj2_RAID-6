import os
import sys
import numpy as np
import time
import os
import numpy
import pandas
import matplotlib.pyplot as plt
from raid.utils import Config, File, RAID6, Timer, read_data, write_data

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CURRENT_PATH)

class TimeEvaluation(object):
    def __init__(self, config):
        self.timer = Timer()
        
        self.print_spliter()
        print(" "*9+"Start the Time Evaluation pipeline!")
        config['data_dir'] = os.path.join(CURRENT_PATH, config['data_dir'])
        config['test_dir'] = os.path.join(CURRENT_PATH, config['test_dir'])

        self.config = config
        
        if config["timer_mode"] == 0:
            self.mode_name = "write time"
            test_pipeline = self.evaluate_write_time(config)
        elif config["timer_mode"] == 1:
            self.mode_name = "read time"
            test_pipeline = self.evaluate_read_time(config)
        elif config["timer_mode"] == 2:
            self.mode_name = "rebuild time"
            test_pipeline = self.evaluate_rebuild_time(config)
            
        if config["mode"] == 1:
            data_generation = self.prepare_real_data
        else:
            data_generation = self.prepare_synthetic_data
        
        group_size_list, time_cost_list = self.evaluation(test_pipeline, data_generation)
    
    def visualization(self, group_size_list, time_cost_list):
        plt.plot(group_size_list, time_cost_list, 'ro')
        plt.xlabel('group size')
        plt.ylabel('time cost')
        plt.title('Time Evaluation for %s' % self.mode_name)
        plt.show() 
        plt.savefig(f"{self.mode_name}_evaluation.png")
        
    def evaluation(self, test_pipeline, data_generation):
        group_size_list = self.config["group_size_list"]
        time_cost_list = []
        for group_size in group_size_list:
            config=self.config
            config["stripe_size"] = group_size*config["data_disk_num"]
            self.raid_controller = RAID6(config)
            self.data_blocks, self.content_size, self.total_stripe = data_generation(config)
            self.init_raid_controller()
            time_cost = test_pipeline()
            time_cost_list.append(time_cost)
        return group_size_list, time_cost_list
            
    def init_raid_controller(self):
        self.raid_controller.set_content_size(self.content_size)
        self.raid_controller.set_total_stripe(self.total_stripe)
        self.raid_controller.write_to_disk(self.data_blocks)

    def print_spliter(self):
        print("=============================================")

    def prepare_synthetic_data(self, config):
        file = File(1)
        file.generate_random_data(config["random_data_size"])
        raw_data = file.get_content()
        print("raw_data: ", raw_data)
        data_blocks, content_size, total_stripe = split_data(raw_data)
        return data_blocks, content_size, total_stripe

    def prepare_real_data(self, config):
        data = read_data(os.path.join(
            config['data_dir'], "real_data/")+config["real_file_name"])
        data_blocks, content_size, total_stripe = split_data(data)
        return data_blocks, content_size, total_stripe

    def evaluate_write_time(self):
        self.timer.start()
        self.raid_controller.write_to_disk(self.data_blocks)
        elapsed_time = self.timer.stop()
        return elapsed_time
    
    def evaluate_read_time(self):
        self.timer.start()
        self.raid_controller.read_from_disk()
        elapsed_time = self.timer.stop()
        return elapsed_time
    
    def evaluate_rebuild_time(self):
        corrupted_disks_list = [0, 5]
        self.raid_controller.corrupt_disk(corrupted_disks_list)
        
        self.timer.start()
        self.raid_controller.rebuild_disk(corrupted_disks_list)
        elapsed_time = self.timer.stop()
        return elapsed_time


if __name__ == "__main__":
    config = Config("raid/configs/raid6_config.yaml")
    test_raid6 = TimeEvaluation(config.config)



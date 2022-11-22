import os
import sys
import numpy as np
import time
import os
import numpy
import pandas
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker  
from raid.utils import Config, File, RAID6, Timer, read_data, write_data, ROOT_DIR

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(CURRENT_PATH)

class TimeEvaluation(object):
    def __init__(self, config):
        self.timer = Timer()
        
        self.print_spliter()
        print(" "*5+"Start the Time Evaluation pipeline!")
        config['data_dir'] = os.path.join(CURRENT_PATH, config['data_dir'])

        self.config = config
        
        if config["timer_mode"] == 0:
            self.mode_name = "read time"
            test_pipeline = self.evaluate_read_time
        elif config["timer_mode"] == 1:
            self.mode_name = "write time"
            test_pipeline = self.evaluate_write_time
        elif config["timer_mode"] == 2:
            self.mode_name = "rebuild time"
            test_pipeline = self.evaluate_rebuild_time
            
        if config["mode"] == 1:
            get_data_func = self.get_real_data
        else:
            get_data_func = self.get_synthetic_data
        
        group_size_list, time_cost_list = self.evaluation(test_pipeline, get_data_func)
        
        self.visualization(group_size_list, time_cost_list)
    
    def visualization(self, group_size_list, time_cost_list):
        plt.plot(group_size_list, time_cost_list, 'r-')
        plt.xlabel('Group Size')
        plt.ylabel('Time Cost')
        plt.gca().yaxis.set_major_formatter(mticker.FormatStrFormatter('%.1f s'))
        plt.title('Time Evaluation for %s' % self.mode_name)
        plt.show() 
        plt.savefig(os.path.join(self.config['data_dir'], f"{self.mode_name}_evaluation.png"))
        print("Save the visualization to %s" % os.path.join(self.config['data_dir'], f"{self.mode_name}_evaluation.png"))
        print(" "*5+"Time Evaluation pipeline finished!")
        self.print_spliter()
        
    def evaluation(self, test_pipeline, get_data_func):
        group_size_list = self.config["group_size_list"]
        time_cost_list = []
        self.data = get_data_func(self.config)
        for group_size in group_size_list:
            config=self.config
            config["stripe_size"] = group_size*config["data_disks_num"]
            self.raid_controller = RAID6(config)
            time_cost = test_pipeline()
            time_cost_list.append(time_cost)
        return group_size_list, time_cost_list
            

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

    def evaluate_write_time(self):
        self.timer.start()
        self.raid_controller.distribute_to_disks(self.data)
        elapsed_time = self.timer.stop()
        return elapsed_time
    
    def evaluate_read_time(self):
        self.raid_controller.distribute_to_disks(self.data)
        self.timer.start()
        self.raid_controller.read_from_disks()
        elapsed_time = self.timer.stop()
        return elapsed_time
    
    def evaluate_rebuild_time(self):
        self.raid_controller.distribute_to_disks(self.data)
        corrupted_disks_list = [0, 5]
        self.raid_controller.corrupt_disk(corrupted_disks_list)
        
        self.timer.start()
        self.raid_controller.rebuild_disk(corrupted_disks_list)
        elapsed_time = self.timer.stop()
        return elapsed_time

if __name__ == "__main__":
    config = Config("raid/configs/time_evaluation.yaml")
    test_raid6 = TimeEvaluation(config.config)



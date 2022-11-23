import os
import sys
import numpy as np
import time
import os
import numpy
import pandas
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker 
import copy
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
            self.mode_name = "read_time"
            test_pipeline = self.evaluate_read_time
        elif config["timer_mode"] == 1:
            self.mode_name = "write_time"
            test_pipeline = self.evaluate_write_time
        elif config["timer_mode"] == 2:
            self.mode_name = "rebuild_time"
            test_pipeline = self.evaluate_rebuild_time
            
        print(" "*5+"Start {} evaluation!".format(self.mode_name))
            
        if config["mode"] == 1:
            get_data_func = self.get_real_data
            self.data_type = "real"
        else:
            get_data_func = self.get_synthetic_data
            self.data_type = "synthetic"
            
        print(" "*5+"Use {} data!".format("real" if config["mode"] == 1 else "synthetic"))
        
        group_size_list, time_cost_list = self.evaluation(test_pipeline, get_data_func)
        
        self.visualization(group_size_list, time_cost_list)
        
    def to_kb(self, group_size_list):
        group_size_in_byte = list(map(lambda x: x/1024, group_size_list))
        # print("group_size_in_byte: ", group_size_in_byte)
        return group_size_in_byte
        
    def visualization(self, group_size_list, time_cost_list):
        # group_size_byte_list = self.to_byte(group_size_list)
        group_size_list = self.to_kb(group_size_list)
        plt.xlabel('Group Size (byte)')
        plt.ylabel('Time Cost (seconds)')
        plt.gca().yaxis.set_major_formatter(mticker.FormatStrFormatter('%.2f s'))
        plt.gca().xaxis.set_major_formatter(mticker.FormatStrFormatter('%d k'))
        plt.title('Time Evaluation for %s' % self.mode_name)
        plt.plot(group_size_list, time_cost_list, 'r-', label=self.mode_name)
        plt.show() 
        plt.savefig(os.path.join(self.config['data_dir'], f"{self.mode_name}_{self.data_type}_evaluation.png"))
        print("Save the visualization to %s" % os.path.join(self.config['data_dir'], f"{self.mode_name}_evaluation.png"))
        print(" "*5+"Time Evaluation pipeline finished!")
        self.print_spliter()
        plt.cla()
        
    def evaluation(self, test_pipeline, get_data_func):
        group_size_list = self.config["group_size_list"]
        time_cost_list = []
        self.data = get_data_func(self.config)
        # with open(os.path.join(self.config['data_dir'], "synthetic_data"), "wb") as f:
        #     f.write(bytes(self.data))
        for group_size in group_size_list:
            config=self.config
            config["group_size"] = group_size
            config["strip_size"] = config["group_size"]*config["data_disks_num"]
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
        data = data.tobytes()
        data = list(data)
        # print("synthetic_data size: ", len(data))
        return data

    def get_real_data(self, config):
        data = read_data(os.path.join(
            config['data_dir'], "real_data/")+config["real_file_name"])
        return data

    def evaluate_write_time(self):
        data = copy.deepcopy(self.data)
        
        self.timer.start()
        self.raid_controller.distribute_to_disks(data)
        elapsed_time = self.timer.stop()
        return elapsed_time
    
    def evaluate_read_time(self):
        data = copy.deepcopy(self.data)
        self.raid_controller.distribute_to_disks(data)
        
        self.timer.start()
        self.raid_controller.get_content()
        elapsed_time = self.timer.stop()
        return elapsed_time
    
    def evaluate_rebuild_time(self):
        data = copy.deepcopy(self.data)
        self.raid_controller.distribute_to_disks(data)
        
        corrupted_disks_list = [0, 5]
        self.raid_controller.remove_disks(corrupted_disks_list)
        
        self.timer.start()
        self.raid_controller.recover_disks(corrupted_disks_list)
        elapsed_time = self.timer.stop()
        return elapsed_time

if __name__ == "__main__":
    config = Config("raid/configs/time_evaluation.yaml")
    modes = [i for i in range(3)]
    # print(modes)
    test_raid6 = [TimeEvaluation(config.config) for config.config["timer_mode"] in modes]
    # for mode in range(3):
    #     config.config["timer_mode"] = mode
    #     test_raid6 = TimeEvaluation(config.config)



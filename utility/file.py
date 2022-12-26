
import csv
import os
import shutil

import pandas as pd


class File:
    
    def open_file(relative_path):
        file_path = os.path.abspath(os.curdir)+relative_path
        with open(file_path) as o_file:
            reader = csv.reader(o_file)
            return list(reader)
    
    def write_file(relative_path, row):
        file_path = os.path.abspath(os.curdir)+relative_path
        with open(file_path, 'w') as w_file:
            writer = csv.writer(w_file)
            writer.writerow(row)
            
    def append_data(relative_path, row):
        file_path = os.path.abspath(os.curdir)+relative_path
        with open(file_path, 'a') as a_file:
            writer = csv.writer(a_file)
            writer.writerow(row)

    def read_file(relative_path):
        file_path = os.path.abspath(os.curdir)+relative_path
        with open(file_path, 'r') as r_file:
            return  csv.reader(r_file)
        
    
    def move_row(soirce_path, destination_path):
        print('move file')
        
    def remove_file(relative_path):
        file_path = os.path.abspath(os.curdir)+relative_path
        if(os.path.exists(file_path) and os.path.isfile(file_path)):
            os.remove(file_path)
    
    def remove_all_file_inside_directory(dir):
        dir_path = os.path.abspath(os.curdir)+dir
        for f in os.listdir(dir_path):
            os.remove(os.path.join(dir_path, f))
    
    # def open_file_without_header(relative_path):
    #     file_path = os.path.abspath(os.curdir)+relative_path
    #     return pd.read_csv(file_path, header=None)
    
    def create_directory(dir_name):
        directory_path = os.path.abspath(os.curdir)+dir_name
        print(directory_path)
        os.makedirs(os.path.dirname(directory_path), exist_ok=True)
    
    def remove_directory(dir_name):
        directory_path = os.path.abspath(os.curdir)+dir_name
        shutil.rmtree(directory_path)

    
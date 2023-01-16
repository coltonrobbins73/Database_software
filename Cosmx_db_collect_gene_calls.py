
print('Compiling packages...')
from operator import index
from pickle import FALSE
import statistics
import pandas as pd
import re
import numpy as np
from collections import Counter
import os
from scipy import stats
from sqlalchemy import create_engine
import mysql.connector
import sqlalchemy
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import exc
from mysql.connector.errors import ProgrammingError
import sqlite3
import win32api
import win32wnet, win32netcon
from pandas.errors import EmptyDataError 
import datetime as dt
from statistics import mode
import statistics
import json
from statistics import median
import csv

def wnet_connect(host, username, password):
    '''Initiates a connection with another work station'''
    unc = ''.join(['\\\\', host])
    win32wnet.WNetAddConnection2(0, None, unc, None, username, password)

class FindFiles:
    '''Generates a list of file paths that match key words'''
    def __init__(self, input_directory):
        self.input_directory = input_directory
        self.paths, self.directory_size = self.__find_files()
    def __find_files(self):
        # Init protein run flagging variable and set to false
        global protein_check
        protein_check = False
        paths = {}
        # Init directory size variable
        directory_size = 0
        # Regex for specific files of interest that we need for analysis and protein run flagging
        check_re_fun = re.compile(r'Target_LookUpTable.csv|complete_code_cell_target_call_coord|target_call_coord.csv')
        # Use os to crawl through directory tree and extract specific files from run folders
        for root, dirs, files in os.walk(self.input_directory, topdown=False):
            for name in files:
                # print(name)
                dir_or_file_path = os.path.join(root, name)
                # skip if it is symbolic link
                if not os.path.islink(dir_or_file_path):
                    try:
                        directory_size += os.path.getsize(dir_or_file_path)
                    except FileNotFoundError:
                        pass
                checked = check_re_fun.search(root + name)
                # Dict key is the full path and value is just the file name
                if checked is not None:
                    paths[root + '\\' + name] = name

        return paths, directory_size

class LabelFiles:
    '''Takes file path list from FindFiles and labels them with specific identifiers organized into a dataframe or nested dictionary'''
    def __init__(self, file_paths):

        self.paths_df, self.lookup_paths = self.label_files(file_paths)

    def compare_file_size_dict_format(self, path_dict, run, slide, full_path):
        if run not in path_dict:
            path_dict[run] = {}                    
        if slide not in path_dict[run]:
            path_dict[run][slide] = full_path
        if path_dict[run][slide]:
            old_file_size = os.stat(path_dict[run][slide])
            new_file_size = os.stat(full_path)
            if new_file_size.st_size > old_file_size.st_size:
                path_dict[run][slide] = full_path
            else:
                pass
        
        return path_dict

    def compare_file_size_df_format(self, paths_df, run, slide, fov, column, full_path):
        
        if fov in paths_df[run][slide].index:
            if pd.isna(paths_df[run][slide].loc[fov, column]):
                paths_df[run][slide].at[fov, column] = full_path
            else:
                old_file_size = os.stat(paths_df[run][slide].loc[fov, column])
                new_file_size = os.stat(full_path)
                if new_file_size.st_size > old_file_size.st_size:
                    paths_df[run][slide].at[fov, column] = full_path
                else:
                    pass
        else:
            paths_df[run][slide].at[fov, column] = full_path
        
        return paths_df

    def label_files(self, file_paths):
        # Init regex function to find files of interest
        check_re_fun = re.compile(r'complete_code_cell_target_call_coord|target_call_coord.csv|Target_LookUpTable.csv')

        run_re_fun = re.compile(r'Run\d{4}A|Run\d{4}|RunA\d{4}|RunB\d{4}|A\d{4}|B\d{4}|R\d{4}')
        slide_re_fun = re.compile(r'_S\d{1}|S\d{1}')
        fov_re_fun = re.compile(r'FOV\d{3}|FOV\d{1}')
        f_re_fun = re.compile(r'F\d{3}')
        f_single_num_re_fun = re.compile(r'FOV[0-9]+')

        lookup_paths = {}
        paths_df = {}

        for file in file_paths:
            # Make sure path is in string format for regex
            full_path = str(file)
            file_name = str(file_paths[file])
            # Check that target keyword is in the path
            check = check_re_fun.search(full_path)
            if check is None:
                continue
            # Search for the run, slide, and fov number hidden within the file path
            run = run_re_fun.search(full_path)
            run = run.group()
            slide = slide_re_fun.search(full_path)
            # Sometimes slide numbers are not included especially for single slide runs, just assign them to S1
            if slide is None:
                slide = 'S1'
            else:    
                slide = slide.group()
            # If the alternative regex for underscore + slide number is used, delete the underscore
            if len(slide) == 3:
                slide = slide[1:]     
            # Select the correct fov format based on target search
            if any(identifier in full_path for identifier in ['complete_code_cell_target_call_coord']):
                fov = fov_re_fun.search(file_name)
                try:
                    fov = fov.group()
                    fov = fov[3:]
                    fov = 'F' + fov
                except AttributeError:
                    fov = f_re_fun.search(file_name)
                    fov = fov.group()
            if any(identifier in full_path for identifier in ['target_call_coord.csv']):
                fov = fov_re_fun.search(file_name)
                try:
                    fov = fov.group()
                    fov = fov[3:]
                    fov = 'F' + fov
                except AttributeError:
                    fov = f_re_fun.search(file_name)
                    fov = fov.group()
            if 'Target_LookUpTable.csv' in file_name:
                lookup_paths = self.compare_file_size_dict_format(lookup_paths, run, slide, full_path)
            else:
                if run not in paths_df:
                    paths_df[run] = {}
                if slide not in paths_df[run]:
                    paths_df[run][slide] = pd.DataFrame(columns = ['ccc', 'tcc'])
                # for col in paths_df[run][slide]:
                #     if paths_df[run][slide][col].eq(full_path).any():
                #         continue
                if 'complete_code_cell_target_call_coord' in file_name:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'ccc', full_path = full_path)
                elif 'target_call_coord.csv' in file_name:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'tcc', full_path = full_path)

        return paths_df, lookup_paths

class ScrapeTranscriptStats:
    '''Scrape various values from run output into more accesible readout'''
    def __init__(self, paths, run_label, origin_path, directory_size):

        print('\nGenerating summary files...')

        # Init master slide df and master fov df to summarize entire run, slides, and fov in a single table
        self.master_slide_df = pd.DataFrame()
        self.master_fov_df = {}
        self.targ_obs = {}

        # Label files using path identifiers
        self.labeled = LabelFiles(file_paths = paths)

        self.scrape_voting(origin_path, run_label)

    def scrape_voting(self, origin_path, run_label):
        '''Iterate over all voting summary files and combine values into one DataFrame'''
        if self.labeled.paths_df:
            print('\nProcessing voted data, this may take a moment...')
            # Make some dictionaries to hold all values from each FOV per category to make quick FOV comparisons
            all_fov_targ_obs = {}
            for run in self.labeled.paths_df:
                if run in self.labeled.lookup_paths:
                    for slide in self.labeled.lookup_paths[run]:
                        lookup = pd.read_csv(self.labeled.lookup_paths[run][slide], engine = 'pyarrow')
                        lookup = lookup[['target_label', 'BC_SpotColumn_1', 'BC_SpotColumn_2', 'BC_SpotColumn_3', 'BC_SpotColumn_4']]
                    for slide in self.labeled.paths_df[run]:
                        self.targ_obs[slide] = pd.DataFrame()

                        # Read in each CCC file and add up all the calls per gene
                        # Should look like this -> {'COL1A1': 7, 'DGKA': 2, 'GZMH': 11...}
                        for index_label in self.labeled.paths_df[run][slide].index:
                            if pd.isnull(self.labeled.paths_df[run][slide]['ccc'][index_label]) and not pd.isnull(self.labeled.paths_df[run][slide]['tcc'][index_label]):
                                self.labeled.paths_df[run][slide].at[index_label, 'ccc'] = self.labeled.paths_df[run][slide]['tcc'][index_label]
                        ccc_list = self.labeled.paths_df[run][slide]['ccc'].loc[~pd.isnull(self.labeled.paths_df[run][slide]['ccc'])]
                        
                        target_obs = {}
                        for fov in ccc_list.index:
                            try:
                                target_calls_df = pd.read_csv(ccc_list[fov], on_bad_lines = 'skip')
                            except EmptyDataError:
                                pass

                            target_calls_df = target_calls_df[['fov', 'target', 'Spot1_count', 'Spot2_count', 'Spot3_count', 'Spot4_count']]
                            target_calls_df['Slide'] = slide
                            target_calls_df['Run'] = run

                            for row in range(len(lookup)):
                                gene_subset = target_calls_df[target_calls_df['target'] == lookup['target_label'][row]]
                                gene_subset = gene_subset.rename({'Spot1_count': lookup['BC_SpotColumn_1'][row], 'Spot2_count': lookup['BC_SpotColumn_2'][row], 'Spot3_count': lookup['BC_SpotColumn_3'][row], 'Spot4_count': lookup['BC_SpotColumn_4'][row]}, axis=1)
                                target_obs[lookup['target_label'][row] + '_' + lookup['BC_SpotColumn_1'][row][-1] + lookup['BC_SpotColumn_2'][row][-1] + lookup['BC_SpotColumn_3'][row][-1] + lookup['BC_SpotColumn_4'][row][-1]] = gene_subset

                            for gene in target_obs:
                                if gene not in all_fov_targ_obs:
                                    all_fov_targ_obs[gene] = target_obs[gene]
                                else:
                                    all_fov_targ_obs[gene] = pd.concat([all_fov_targ_obs[gene], target_obs[gene]], ignore_index = True)

                for gene in all_fov_targ_obs:
                    all_fov_targ_obs[gene].to_csv('K:\\Target_observation_analysis\\Full\\All_targ_obs\\{}-{}.csv'.format(run, gene.replace(r'/','-')))

        return

if __name__ == "__main__":

    connection = 'dummy_string'

    run_list_df = pd.read_excel(r'K:\Target_observation_analysis\Full\Query_list_processed.xlsx')
    run_regex = re.compile(r'Run\d{4}A|Run\d{4}|RunA\d{4}|RunB\d{4}|A\d{4}|R\d{4}')

    run_list = list(set(run_list_df['Origin_path']))
    drive_re_fun = re.compile(r'(?<=\\\\).*?(?=\\)')

    for path in run_list:
        if connection not in path:
            connection = drive_re_fun.search(path).group()
            try:
                wnet_connect('{}'.format(connection), 'project', 'charlie13')
            except:
                try:
                    wnet_connect('{}'.format(connection), 'Nsadmin', 'BetaBetaBeta!')
                except:
                    pass
        print('\nCompiling all file paths for *****{}*****'.format(path))
        run_label = run_regex.search(path).group()
        # Compile file paths for each run directory
        all_files = FindFiles(input_directory = path)
        # Scrape for protein information or transcript information
        ScrapeTranscriptStats(paths = all_files.paths, run_label = run_label, origin_path = path, directory_size = all_files.directory_size)        
        new_run_list = [x for x in run_list if x != path]
        processed_df = pd.DataFrame(new_run_list, columns = ['Origin_path'])
        processed_df.to_excel(r'K:\Target_observation_analysis\Full\Query_list_processed.xlsx')



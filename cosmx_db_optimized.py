'''
CosMx database compilation tool

Colton Robbins (crobbins@nanostring.com)
9/7/2022

1) Walk through all CosMx drives
2) Identify RNA and Protein run directories
3) Cross-reference existing SQL database entries
4) Identify data files within each run
5) Compile data values into master tables
6) Upload new entries into SQL database

'''

print('Compiling packages...')
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

def wnet_connect(host, username, password):
    '''Initiates a connection with another work station'''
    unc = ''.join(['\\\\', host])
    win32wnet.WNetAddConnection2(0, None, unc, None, username, password)

def scrape_cellstats(labeled, master_fov_df, master_slide_df, timestamp_readable):
    '''Iterate over all cell stats summary files to append to master dataframe'''
    # Init recieving dictionaries
    cellsumm_perslide = {}
    data_load_df = {}
    # Check if we found any cell stats summary files
    if labeled.paths_df:
        print('\nScraping CellStatsDir...')
        # Open each CellStatSummary csv file and scrape information
        for run in labeled.paths_df:
            for slide in labeled.paths_df[run]:
                # Init recieving dataframes
                perFOV_cellsumm_df = pd.DataFrame()
                if slide not in master_fov_df:
                    master_fov_df[slide] = pd.DataFrame()
                data_load_df[slide] = pd.DataFrame()

                # Isolate true paths from labeling dataframe
                cell_stats_list = labeled.paths_df[run][slide]['cell_stats'].loc[~pd.isnull(labeled.paths_df[run][slide]['cell_stats'])]
                for fov in cell_stats_list.index:
                    # If run start date is not present, take the timestamp from this file
                    if timestamp_readable is None:
                        timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(cell_stats_list[fov]))
                        timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")
                    # Parse cellstats summary csv and convert to dataframe
                    cell_stats = ParseFile(cell_stats_list[fov], fov_label = fov)
                    perFOV_cellsumm_df = pd.concat([perFOV_cellsumm_df, cell_stats.parsed])


                data_load_df[slide] = perFOV_cellsumm_df
                cellsumm_perslide[slide] = perFOV_cellsumm_df
                # Only take the first 4 characters of cellsumm_perslide for each index value 
                new_index = [index[:4] for index in cellsumm_perslide[slide].index]
                cellsumm_perslide[slide].index = new_index
            # Load scraped values into master dataframes
            master_fov_df, master_slide_df = scrape_to_summary(data_load_df, master_fov_df, master_slide_df, run)

    return master_fov_df, master_slide_df, cellsumm_perslide, timestamp_readable # Need cellsumm_perslide variable for total cell number to get per cell stats during voting analysis

def scrape_config(labeled, master_fov_df, master_slide_df, timestamp_readable):
    '''Scrape experiment information from config file to append to master dataframe'''
    # Check if there are any config file paths
    instrument = None
    if labeled.config:
        print('\n\nProcessing config file...')
        # Init recieving dictionary and instrument label
        data_load_df = {}
        for run in labeled.config:
            for slide in labeled.config[run]:
                data_load_df[slide] = pd.DataFrame()    
                fov = 'F001' # Dummy value so ParseFile can be used
                # If run start date is not present, take the timestamp from this file
                if timestamp_readable is None:
                    timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(labeled.config[run][slide]))
                    timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")
                # Parse config txt and convert to dataframe
                config_stats = ParseFile(labeled.config[run][slide], fov_label = fov, config = True)
                data_load_df[slide] = pd.concat([data_load_df[slide], config_stats.config])

            # Load scraped values into master dataframes
            master_fov_df, master_slide_df = scrape_to_summary(data_load_df, master_fov_df, master_slide_df, run, config = True)
            instrument = list(set(data_load_df[slide]['Instrument']))[0]
            
    return master_fov_df, master_slide_df, instrument, timestamp_readable # Need instrument label for calculating spot density per um^2 in scrape_spatial function

def scrape_to_summary(data_load_df, master_fov_df, master_slide_df, run, config = False, log = False):
    '''Transfers data from receiving dictionary to master dataframe'''
    for slide in data_load_df:
        # Init new slide dataframe if not already present
        if slide not in master_fov_df:
            master_fov_df[slide] = pd.DataFrame()
        # Config file contains data for the whole run so we just load into per slide dataframe and will transfer to per fov dataframe later
        if config == True:
            for col in data_load_df[slide].columns:
                master_slide_df.at[run + '_' + slide, col] = data_load_df[slide][col][0]
        # Log file contains data for the whole run so we just load into per slide dataframe and will transfer to per fov dataframe later
        if log == True:
            for col in data_load_df[slide].columns:
                master_slide_df.at[run + '_' + slide, col] = data_load_df[slide][col][slide]
        elif config == False:
            run_slide_fov_list = list()
            # Modify index with run and slide labels
            for fov in data_load_df[slide].index:
                run_slide_fov_list.append(run + '_' + slide + '_' + fov)
            data_load_df[slide].index = run_slide_fov_list
            # Transfer data to master fov dataframe
            for col in data_load_df[slide].columns:
                master_fov_df[slide][col] = data_load_df[slide][col]
            # Take the mean and standard deviation of all fovs to load into per slide datafrmae
            for col in data_load_df[slide].columns:
                master_slide_df.at[run + '_' + slide, col] = data_load_df[slide][col].mean()
                master_slide_df.at[run + '_' + slide, col + '_sd'] = data_load_df[slide][col].std()

    return master_fov_df, master_slide_df

def scrape_log(labeled, master_fov_df, master_slide_df, timestamp_readable):
    '''Scrape user information from restart token file to append to master dataframe'''

    def pull_owner_from_jason(json_list, data_load_df, slide):
        '''Function to parse various json forms for actual user name'''
        # Regex for keyword identifiers 
        owner_re_fun = re.compile(r'Responsible User|Enter email ids of run owners|RunOwnersEmail|UserEmail')
        comma_period_re_fun = re.compile(r',|\.')
        # Iterate through list of json elements and extract user entry element
        for element_num in range(len(json_list)):
            found_owner_hit = owner_re_fun.search(json_list[element_num]['name'])
            if found_owner_hit is not None:
                owner = json_list[element_num]['value']
        # Check if there is multiple users listed to be placed in seperate columns
        sep_check = comma_period_re_fun.search(owner)
        if sep_check is not None:
            owner_list = owner.split(',')
            if isinstance(owner_list, list):
                for owner_num in range(len(owner_list)):
                    data_load_df[slide].at[slide, 'Owner_' + str(owner_num + 1)] = owner_list[owner_num]
            else:
                owner_list = owner.split('.')
                if isinstance(owner_list, list):
                    for owner_num in range(len(owner_list)):
                        data_load_df[slide].at[slide, 'Owner_' + str(owner_num + 1)] = owner_list[owner_num]
        else:
            data_load_df[slide].at[slide, 'Owner_1'] = owner
        
        return data_load_df

    # Check if there are any restart_token paths
    if labeled.log_paths:
        print('Processing config file...')
        # Init recieving dictionary
        data_load_df = {}        
        for run in labeled.log_paths:
            for slide in labeled.log_paths[run]:
                data_load_df[slide] = pd.DataFrame()    
                # If run start date is not present, take the timestamp from this file
                if timestamp_readable is None:
                    timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(labeled.log_paths[run][slide]))
                    timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")
                # Open json file and load into dictionary
                json_file = open(labeled.log_paths[run][slide])
                json_dict = json.load(json_file)
                # Extract user information from various json formats
                try:
                    if isinstance(json_dict['TaskConfig'], list):
                        data_load_df = pull_owner_from_jason(json_dict['TaskConfig'], data_load_df, slide)
                except TypeError:
                    if isinstance(json_dict, list):
                        data_load_df = pull_owner_from_jason(json_dict, data_load_df, slide)

            # Load scraped values into master dataframes
            master_fov_df, master_slide_df = scrape_to_summary(data_load_df, master_fov_df, master_slide_df, run, log = True)
            
    return master_fov_df, master_slide_df, timestamp_readable

class FromSQL:
    '''Get list of already processed runs from SQL database'''
    def __init__(self):
        print('Retreiving previously compiled runs...')
        # Init global vars to flag if we need to make a new SQL table
        global new_fov_transcript_table
        global new_slide_transcript_table
        global new_fov_protein_table
        global new_slide_protein_table
        # Check for previously generated tables and recieve compiled lists 
        self.compiled_transcript, new_slide_transcript_table = self.get_previous_entries('all_slide_transcript_v5')
        new_fov_transcript_table = self.check_for_table('all_fov_transcript_v5')
        self.compiled_protein, new_slide_protein_table = self.get_previous_entries('all_slide_protein_v2')
        new_fov_protein_table = self.check_for_table('all_fov_protein_v2')

    def check_for_table(self, table_name):
        # Connect to MySQL server
        creds = {'usr': 'root',
                'pwd': 'charlie13',
                'hst': 'localhost',
                'prt': 3306,
                'dbn': 'cosmxdb'}
        connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
        engine = create_engine(connstr.format(**creds))
        # Read in origin path names from per slide table to generate list of already analyzed runs
        # Update the global new_table variable 
        try:
            df = pd.read_sql_query('''SELECT Origin_path FROM {}'''.format(table_name), con=engine)
            new_table_check = False
        except:
            new_table_check = True

        return new_table_check

    def get_previous_entries(self, table_name):
        # Connect to MySQL server
        compiled = list()
        creds = {'usr': 'root',
                'pwd': 'charlie13',
                'hst': 'localhost',
                'prt': 3306,
                'dbn': 'cosmxdb'}
        connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
        engine = create_engine(connstr.format(**creds))
        # Read in origin path names from per slide table to generate list of already analyzed runs
        # Update the global new_table variable 
        try:
            df = pd.read_sql_query('''SELECT Origin_path FROM {}'''.format(table_name), con=engine)
            for row in range(len(df)):
                run_index = df['Origin_path'][row]
                run_index = run_index.decode('utf-8')
                compiled.append(run_index)
            compiled = list(set(compiled))
            new_table_check = False
        except:
            print('No slide table created. Skipping previously compiled check...')
            compiled = ['Fresh_table_being_made', 'no_need_for_pre-compiled_list']
            new_table_check = True

        return compiled, new_table_check

class ToSQL:
    '''Connect to a SQL server and upload master dataframes after run analysis'''
    def __init__(self, df, target_table, export_label, new_table, id_columns):
        # Connect to MySQL server
        creds = {'usr': 'root',
                'pwd': 'charlie13',
                'hst': 'localhost',
                'prt': 3306,
                'dbn': 'COSMXDB'}
        connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
        engine = create_engine(connstr.format(**creds))
        # Replace any dashes with underscores because SQL throws errors
        no_dash_col_list = [column.replace('-', '_') for column in list(df.columns)]
        df.columns = no_dash_col_list
        # If table exists, get table columns from database
        if new_table == False:
            try:
                current_cols = pd.read_sql_query('''SELECT COLUMN_NAME 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE 
                    TABLE_SCHEMA = 'cosmxdb'
                AND TABLE_NAME = '{}'
                '''.format(target_table), con=engine)
            except mysql.connector.errors.ProgrammingError:
                print('No table created')
            # Intersect current and incoming column names to find new column names
            new_col_list = list(df.columns)
            old_col_list = current_cols['COLUMN_NAME'].tolist()
            col_to_append = list()
            for col in new_col_list:
                if col not in old_col_list:
                    col_to_append.append(col)

            # Establish connection to MySQL database
            mydb = mysql.connector.connect(
                host = "localhost",
                user = "root",
                password = "charlie13",
                database = "cosmxdb"
            )
            # Create a cursor object to alter the table with new columns as necessary
            mycursor = mydb.cursor()
            for col in col_to_append:
                if df[col].dtype == object:       
                    query = "ALTER TABLE {} ADD {} TEXT;".format(target_table, col)
                    mycursor.execute(query)          
                else:                      
                    query = "ALTER TABLE {} ADD {} FLOAT;".format(target_table, col)
                    mycursor.execute(query)
        else:
            # Update df to final_df immedietely if the table does not already exist
            final_df = df

        # Retreive previous row entries in the run ID and origin_path columns
        # Merge previous entries with new entries then take only the new unique entry values 
        # This is one last referential integrity check when scraping the same directory multiple times
        if new_table == False:
            previous_entries_df = pd.read_sql_query('''SELECT {}, {} FROM {}'''.format(id_columns[0], id_columns[1], target_table), con=engine)
            final_df = pd.merge(df, previous_entries_df, on = id_columns, how="outer", indicator=True
                        ).query('_merge=="left_only"')
            final_df = final_df.drop('_merge', axis = 1)

        submit_success = False
        # Write DataFrame to MySQL table
        for i in range(len(final_df)):
            try:
                final_df.iloc[i:i+1].to_sql(name= target_table ,if_exists='append',con = engine, index = False)
                submit_success = True
            except:
                pass
        # Check if entries were uploaded
        if submit_success == True:
            print('\nUploaded entry to database -----{}-----'.format(export_label))
        else:
            print('\nEntry not submitted ______{}______'.format(export_label))
        self.created = True

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
        check_re_fun = re.compile(r'complete_code_cell_target_call_coord|CellStatsDir|SpatialBC|_Analysis_Summary.txt|CurrentSpotLists|ExptConfig.txt|perCell_1ChStats.csv|restart_token.json|analysis_params_v6|analysis_params_qscore|target_call_coord.csv|Target_LookUpTable.csv')
        protein_run_check_re = re.compile(r'ProteinDir')
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
                protein_run_check = protein_run_check_re.search(root + name)
                # Update protein run flag if "ProteinDir" is present
                if protein_run_check is not None:
                    protein_check = True
                # Dict key is the full path and value is just the file name
                if checked is not None:
                    paths[root + '\\' + name] = name

        return paths, directory_size

class ToExcel:
    '''Send dataframes to excel either in one sheet per file or multiple sheets per file'''
    def __init__(self, df, save_directory, multi_sheet = False):
        if multi_sheet == True:
            sheets = list(df.keys())
            writer = pd.ExcelWriter(save_directory, engine='xlsxwriter')
            for sheet in sheets:
                df[sheet].to_excel(writer, sheet_name = sheet, index = False)
            writer.save()
        else:
            writer = pd.ExcelWriter(save_directory, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='All_slides_summary')
            writer.save()

class LabelFiles:
    '''Takes file path list from FindFiles and labels them with specific identifiers organized into a dataframe or nested dictionary'''
    def __init__(self, file_paths):

        self.paths_df, self.spatial, self.config, self.protein_paths, self.log_paths, self.vote_version, self.plex = self.label_files(file_paths)

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
        check_re_fun = re.compile(r'complete_code_cell_target_call_coord|CellStatsDir|SpatialBC|_Analysis_Summary.txt|CurrentSpotLists|ExptConfig.txt|perCell_1ChStats.csv|restart_token.json|analysis_params_v6|analysis_params_qscore|target_call_coord.csv|Target_LookUpTable.csv')
        # Init
        cell_stat_check_re_fun = re.compile(r'(?=.*CellStatsDir)(?=.*Summary)')
        log_check_re_fun = re.compile((r'(?=.*SpatialProfiling)(?=.*restart_token.json)'))

        # General Regex functions to be combined in different ways for specific file types
        exclusion_re_fun = re.compile(r'Metrics3D|.pdf|.png|after_Z|.pickle|.pb|cellMatrix|Thumbs|.db|edited|Copy|TAPDataComp|Manual Segmentation|Summary_Compiled')
        run_re_fun = re.compile(r'Run\d{4}A|Run\d{4}|RunA\d{4}|RunB\d{4}|A\d{4}|B\d{4}|R\d{4}')
        slide_re_fun = re.compile(r'_S\d{1}|S\d{1}')
        cycle_re_fun = re.compile(r'C\d{3}')
        fov_re_fun = re.compile(r'FOV\d{3}|FOV\d{1}')
        f_re_fun = re.compile(r'F\d{3}')
        f_single_num_re_fun = re.compile(r'FOV[0-9]+')
        protein_gene_re_fun = re.compile(r'(F\d{3}_)(.*)(_perCell_1ChStats)')
        vote_version_re_fun = re.compile(r'_v5|_v6')

        spatial_paths = {}
        paths_df = {}
        config_paths = {}
        plex_paths = {}
        log_paths = {}
        color_code_paths = {}
        protein_paths_df = {}
        vote_version = 'Unknown'

        for file in file_paths:
            # Make sure path is in string format for regex
            full_path = str(file)
            file_name = str(file_paths[file])
            # Check that target keyword is in the path
            check = check_re_fun.search(full_path)
            if check is None:
                continue
            # Check that there are not some stubborn similar file types that also match keywords
            exclusion = exclusion_re_fun.search(full_path)
            if exclusion is not None:
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
            # File specific identifier scraping
            cell_stat_check = cell_stat_check_re_fun.search(full_path)
            if cell_stat_check is not None or 'CurrentSpotLists.txt' in file_name:
                fov = f_re_fun.search(file_name)
                fov = fov.group()  
            if 'perCell_1ChStats.csv' in file_name:
                fov = f_re_fun.search(file_name)
                fov = fov.group() 
                protein_gene = protein_gene_re_fun.search(file_name)
                protein_gene = protein_gene.group(2).upper()         
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
                    # fov = fov[3:]
                    # if int(fov) > 99:
                    #     fov = 'F' + str(int(fov))
                    # elif int(fov) > 9:
                    #     fov = 'F' + '0' + str(int(fov))
                    # elif int(fov) < 10:
                    #     fov = 'F' + '00' + str(int(fov))
                if vote_version == 'Unknown':
                    vote = vote_version_re_fun.search(full_path)
                    if vote is not None:
                        vote_version = vote.group()[1:]
            if any(identifier in full_path for identifier in ['target_call_coord.csv']):
                fov = fov_re_fun.search(file_name)
                try:
                    fov = fov.group()
                    fov = fov[3:]
                    fov = 'F' + fov
                except AttributeError:
                    fov = f_re_fun.search(file_name)
                    fov = fov.group()
                    # fov = fov[3:]
                    # if int(fov) > 99:
                    #     fov = 'F' + str(int(fov))
                    # elif int(fov) > 9:
                    #     fov = 'F' + '0' + str(int(fov))
                    # elif int(fov) < 10:
                    #     fov = 'F' + '00' + str(int(fov))
            if any(identifier in full_path for identifier in ['_Analysis_Summary.txt']):
                fov = fov_re_fun.search(file_name)
                fov = fov.group()
                if 'FOV' in fov:
                    fov = f_single_num_re_fun.search(file_name)
                    fov = fov.group()
                    fov = fov[3:]
                    if int(fov) > 99:
                        fov = 'F' + str(int(fov))
                    elif int(fov) > 9:
                        fov = 'F' + '0' + str(int(fov))
                    elif int(fov) < 10:
                        fov = 'F' + '00' + str(int(fov))
            if 'SpatialBC' in file_name:
                if run not in spatial_paths:
                    spatial_paths[run] = {}                    
                if slide not in spatial_paths[run]:
                    spatial_paths[run][slide] = {}
                if len(spatial_paths[run][slide]) == 0:
                    spatial_paths[run][slide][0] = full_path
                else:
                    for path_num in range(len(spatial_paths[run][slide])):
                        spatial_paths[run][slide][path_num + 1] = full_path

                # if spatial_paths[run][slide]:
                #     old_file_size = os.stat(path_dict[run][slide])
                #     new_file_size = os.stat(full_path)
                #     if new_file_size.st_size > old_file_size.st_size:
                #         path_dict[run][slide] = full_path
                #     else:
                #         pass
            elif 'ExptConfig.txt' in file_name:
                config_paths = self.compare_file_size_dict_format(config_paths, run, slide, full_path)
            elif 'analysis_params_v6' in file_name or 'analysis_params_qscore' in file_name:
                plex_paths = self.compare_file_size_dict_format(plex_paths, run, slide, full_path)
                if vote_version == 'Unknown':
                    vote = vote_version_re_fun.search(file_name)
                    if vote is not None:
                        vote_version = vote.group()[1:]
            elif 'Target_LookUpTable.csv' in file_name:
                color_code_paths = self.compare_file_size_dict_format(color_code_paths, run, slide, full_path)
            elif 'restart_token.json' in file_name:
                log_check = log_check_re_fun.search(file_name)
                if log_check is not None:
                    log_paths = self.compare_file_size_dict_format(log_paths, run, slide, full_path)
            elif 'perCell_1ChStats.csv' in file_name:
                if run not in protein_paths_df:
                    protein_paths_df[run] = {}
                if slide not in protein_paths_df[run]:
                    protein_paths_df[run][slide] = pd.DataFrame()
                if protein_gene not in list(protein_paths_df[run][slide].columns):
                    protein_paths_df[run][slide].at[fov, protein_gene] = full_path
                if fov not in list(protein_paths_df[run][slide].index):
                    protein_paths_df[run][slide].at[fov, protein_gene] = full_path
                if pd.isna(protein_paths_df[run][slide].loc[fov, protein_gene]):
                    protein_paths_df[run][slide].at[fov, protein_gene] = full_path
            else:
                if run not in paths_df:
                    paths_df[run] = {}
                if slide not in paths_df[run]:
                    paths_df[run][slide] = pd.DataFrame(columns = ['ccc', 'tcc', 'match_eff', 'cell_stats', 'spot_file'])
                # for col in paths_df[run][slide]:
                #     if paths_df[run][slide][col].eq(full_path).any():
                #         continue
                if 'complete_code_cell_target_call_coord' in file_name:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'ccc', full_path = full_path)
                elif 'target_call_coord.csv' in file_name:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'tcc', full_path = full_path)
                elif '_Analysis_Summary' in file_name:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'match_eff', full_path = full_path)
                elif cell_stat_check is not None:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'cell_stats', full_path = full_path)
                elif 'CurrentSpotLists' in file_name:
                    paths_df = self.compare_file_size_df_format(paths_df, run, slide, fov, column = 'spot_file', full_path = full_path)

        return paths_df, spatial_paths, config_paths, protein_paths_df, log_paths, vote_version, plex_paths

class ParseFile:
    def __init__(self, file_path, fov_label, config = False, plex = False):
        '''Reads a text file and then searches for regex keywords line by line'''
        print(file_path)
        self.parsed, self.config, self.plex = self.parse_cellsum_file(file_path = file_path, fov_label = fov_label, config = config, plex = plex)

    def parse_cellsumm_line(self, line, config, plex):
        ''''Search each line of a text file'''
        # Declare all regex search parameters
        if config == False and plex == False:
            rx_dict = {
            'Memstained_cells_percent': re.compile(r'Cells with membrane signal,(?P<Memstained_cells_percent>.*),'),
            'Nucstained_cells_percent': re.compile(r'Cells with nuclei signal,(?P<Nucstained_cells_percent>.*),'),
            # 'Avg_mem_seg_diameter_um': re.compile(r'Avg Membrane Segment,(?P<Avg_mem_seg_diameter_um>.*),'),
            'SBR_mem': re.compile(r'Membrane SBR,(?P<SBR_mem>.*)\n'),
            'SBR_UV': re.compile(r'SBR - UV,(?P<SBR_UV>.*)\n'),
            'SBR_B': re.compile(r'SBR - B,(?P<SBR_B>.*)\n'),
            'SBR_G': re.compile(r'SBR - G,(?P<SBR_G>.*)\n'),
            'SBR_Y': re.compile(r'SBR - Y,(?P<SBR_Y>.*)\n'),
            'SBR_R': re.compile(r'SBR - R,(?P<SBR_R>.*)\n'),
            'Cell_coverage_percent': re.compile(r'Cells coverage,(?P<Cell_coverage_percent>.*),'),
            'Nuc_area_square_um': re.compile(r'Nuclear Area,(?P<Nuc_area_square_um>.*),'),
            'Avg_Cell_area_square_um': re.compile(r'Avg Cell Area,(?P<Avg_Cell_area_square_um>.*),'),
            'Num_cells': re.compile(r'#Cells,(?P<Num_cells>.*)'),
            'Matching_efficiency': re.compile(r'Matching efficiency \(%\): (?P<Matching_efficiency>.*),'),
            }
        elif config == True:
            rx_dict = {
            'Instrument': re.compile(r'Instrument: (?P<Instrument>.*)\n'),
            'SW_version': re.compile(r'SW Version: (?P<SW_version>.*)\n'),
            'Protein_nCoder': re.compile(r'Protein nCoder file:(?P<Protein_nCoder>.*)\n'),
            'Ch_HDR': re.compile(r'Ch HDR  :(?P<Ch_HDR>.*)\n'),
            'Ch_thresh': re.compile(r'Ch Thresh.:(?P<Ch_thresh>.*)\n'),
            'Zsteps': re.compile(r'Z-steps: (?P<Zsteps>.*)\n'),
            'Z_proj_method': re.compile(r'Protein ZProj Method: : (?P<Z_proj_method>.*)\n'),
            'DeltaZ': re.compile(r'NNDecon DeltaZ: (?P<DeltaZ>.*)\n'),
            'Bkg_sub_method': re.compile(r'BkgSubMethod: (?P<Bkg_sub_method>.*)\n'),
            'Decoding_method': re.compile(r'Protein Decoding Method: (?P<Decoding_method>.*)\n'),
            'Ch_norm': re.compile(r'Ch Norm.:(?P<Ch_norm>.*)\n'),
            }
        elif plex == True:
            rx_dict = {
            'Plexity': re.compile(r'spots_to_analyze : (?P<Plexity>.*)\n')
            }
        # Execute regex on each line
        for key, rx in rx_dict.items():
            match = rx.search(line)
            if match:
                return key, match
        # if there are no matches
        return None, None

    def parse_cellsum_file(self, file_path, fov_label, config, plex):
        '''Open text file and read each line'''
        data = pd.DataFrame()
        config_df = pd.DataFrame()
        plex_df = pd.DataFrame()
        # open the file and read through it line by line
        with open(file_path, 'r') as file_object:
            line = file_object.readline()
            while line:
                # at each line check for a match with a regex
                key, match = self.parse_cellsumm_line(line, config, plex)
                if config == False and plex == False:
                    if key is not None:
                        try:
                            data.at[fov_label, key] = float(match.group(key))
                        except ValueError:
                            pass
                    line = file_object.readline()
                elif config == True:
                    if key is not None:
                        try:
                            config_df.at[0, key] = match.group(key)
                        except ValueError:
                            pass
                    line = file_object.readline()
                elif plex == True:
                    if key is not None:
                        try:
                            plexity = match.group(key)
                        except ValueError:
                            pass
                        plexity = plexity.replace('[', '')
                        plexity = plexity.replace(']', '')
                        plexity = plexity.replace(' ', '')
                        plexity = plexity.split(',')
                        if len(plexity) == 8:
                            plexity = 100
                            plex_df.at[0, key] = plexity
                        elif len(plexity) == 16:
                            plexity = 1000
                            plex_df.at[0, key] = plexity
                    line = file_object.readline()
                    
        return data, config_df, plex_df

class ScrapeTranscriptStats:
    '''Scrape various values from run output into more accesible readout'''
    def __init__(self, paths, run_label, origin_path, directory_size):
        
        # Recall previous global variables to check if database tables exist
        global new_fov_transcript_table
        global new_slide_transcript_table
        self.timestamp_readable = None

        print('\nGenerating summary files...')

        # Init master slide df and master fov df to summarize entire run, slides, and fov in a single table
        self.master_slide_df = pd.DataFrame()
        self.master_fov_df = {}
        self.profiling_target_calls = {}

        # Label files using path identifiers
        self.labeled = LabelFiles(file_paths = paths)
        # Process CellStatsDir
        self.master_fov_df, self.master_slide_df, self.cellsumm_perslide, self.timestamp_readable = scrape_cellstats(self.labeled, self.master_fov_df, self.master_slide_df, self.timestamp_readable)
        # Process config file
        self.master_fov_df, self.master_slide_df, self.instrument, self.timestamp_readable = scrape_config(self.labeled, self.master_fov_df, self.master_slide_df, self.timestamp_readable)
        # Process voting params file
        self.scrape_plex()
        # Process log file for run owner
        self.master_fov_df, self.master_slide_df, self.timestamp_readable = scrape_log(self.labeled, self.master_fov_df, self.master_slide_df, self.timestamp_readable)
        # Process raw counts and fiducial metrics
        self.scrape_spatial_bc()
        # Process spot files for z step information
        self.scrape_spot_zstep()
        # Process voting metrics
        self.scrape_voting(origin_path, run_label)

        
        column_constants_list = ['Instrument', 'SW_version', 'Protein_nCoder', 'Ch_HDR', 'Ch_thresh', 
        'Zsteps', 'Z_proj_method', 'DeltaZ', 'Bkg_sub_method', 'Decoding_method',
        'Ch_norm', 'Owner_1', 'Owner_2', 'Owner_3', 'Plexity']

        # Get directory creation date
        if self.timestamp_readable is None:
            timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(origin_path))
            self.timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")
        # Convert directory size to Gb
        dir_size = round(directory_size / 1000000000, 2)

        # Modify origin path forward slashes because SQL doesn't like this character
        # Add origin path as a new column in per slide table
        origin_path = origin_path.replace('\\', '&')   
        if self.master_fov_df:
            for slide in self.master_fov_df:
                if self.master_fov_df[slide].empty:
                    self.master_fov_df[slide].index = [str(run_label) + slide + '_empty']
                self.master_fov_df[slide].insert(0, 'Voting_version', self.labeled.vote_version)
                self.master_fov_df[slide].insert(0, 'Run_directory_size_gb', dir_size)
                self.master_fov_df[slide].insert(0, 'Exp_start_date', self.timestamp_readable)
                self.master_fov_df[slide].insert(0, 'Origin_path', origin_path)
                self.master_fov_df[slide].insert(0, 'Run_slide_fov', self.master_fov_df[slide].index)
                
            for col in column_constants_list:
                if col in self.master_slide_df:
                    col_index = self.master_slide_df.columns.get_loc(col)
                    for slide in self.master_fov_df:
                        self.master_fov_df[slide][col] = self.master_slide_df.iloc[0, col_index]
                    
        # Send per fov table to SQL
            for slide in self.master_fov_df:
                fov_tb = ToSQL(self.master_fov_df[slide], 'all_fov_transcript_v5', str(run_label) + '_' + slide, new_table = new_fov_transcript_table, id_columns = ['Run_slide_fov', 'Origin_path'])
        try:
            # If new table was created flip the global new table variable
            if fov_tb.created == True:
                new_fov_transcript_table = False
        except NameError:
            pass

        if self.master_slide_df.empty:
            self.master_slide_df.index = [str(run_label) + '_S1_empty']
        # Insert the index run label as a new column in the first column position and orgin path as a new column
        self.master_slide_df.insert(0, 'Voting_version', self.labeled.vote_version)
        self.master_slide_df.insert(0, 'Run_directory_size_gb', dir_size)
        self.master_slide_df.insert(0, 'Exp_start_date', self.timestamp_readable)
        self.master_slide_df.insert(0, 'Origin_path', origin_path)
        self.master_slide_df.insert(0, 'Run_slide', self.master_slide_df.index)

        for col in column_constants_list:
            if col in self.master_slide_df:
                col_index = self.master_slide_df.columns.get_loc(col)
                col_series = self.master_slide_df.iloc[:, col_index]
                col_series_value = list(set(col_series.dropna()))
                self.master_slide_df[col] = col_series_value[0]

        # Send per slide dataframe to SQL
        slide_tb = ToSQL(self.master_slide_df, 'all_slide_transcript_v5', str(run_label) + '_per_fov_df', new_table = new_slide_transcript_table, id_columns = ['Run_slide', 'Origin_path'])
        try:
        # If new table was created flip the global new table variable
            if slide_tb.created == True:
                new_slide_transcript_table = False
        except NameError:
            pass

    def scrape_voting(self, origin_path, run_label):
        '''Iterate over all voting summary files and combine values into one DataFrame'''
        if self.labeled.paths_df:
            print('\nProcessing voted data, this may take a moment...')
            # Make some dictionaries to hold all values from each FOV per category to make quick FOV comparisons
            data_load_df = {}
            match_summ = {}
            for run in self.labeled.paths_df:
                for slide in self.labeled.paths_df[run]:
                    self.profiling_target_calls[slide] = pd.DataFrame()
                    # Initialize dataframes/dictionaries per slide for each category
                    data_load_df[slide] = pd.DataFrame()
                    match_summ[slide] = pd.DataFrame()

                    # Read in each CCC file and add up all the calls per gene
                    # Should look like this -> {'COL1A1': 7, 'DGKA': 2, 'GZMH': 11...}
                    for index_label in self.labeled.paths_df[run][slide].index:
                        if pd.isnull(self.labeled.paths_df[run][slide]['ccc'][index_label]) and not pd.isnull(self.labeled.paths_df[run][slide]['tcc'][index_label]):
                            self.labeled.paths_df[run][slide].at[index_label, 'ccc'] = self.labeled.paths_df[run][slide]['tcc'][index_label]
                    ccc_list = self.labeled.paths_df[run][slide]['ccc'].loc[~pd.isnull(self.labeled.paths_df[run][slide]['ccc'])]
                    
                    for fov in ccc_list.index:
                        try:
                            target_calls_df = pd.read_csv(ccc_list[fov], on_bad_lines='skip')
                        except EmptyDataError:
                            pass

                        # Slice away false and neg codes in target df to get true genes
                        try:
                            true_df = target_calls_df[~target_calls_df['target'].str.contains('False')]
                            true_df = true_df[~true_df['target'].str.contains('NegPrb')]
                        # Bad file in \\smi05\H\Run4123_20201120_dash05_EN-RCC-02-1K-LFC-0.001fids-DASH05 (likely NaNs in target column)
                        except TypeError:
                            break
                        # Make seperate false and neg dfs and get list of z slices
                        false_df = target_calls_df[target_calls_df['target'].str.contains('False')]
                        neg_df = target_calls_df[target_calls_df['target'].str.contains('NegPrb')]
                        df_dict = {'true_genes' : true_df, 'false_codes' : false_df, 'neg_probes' : neg_df}

                        gene_profile = true_df.set_index('target')
                        gene_profile.index = gene_profile.index.str.upper()
                        try:
                            gene_profile = gene_profile['target_call_observations']
                            mean_gene_profile = gene_profile.groupby(level=0).mean()
                            mean_gene_profile = mean_gene_profile.rename(run + '_' + slide + '_' + fov)
                            mean_gene_profile = round(mean_gene_profile, 2)
                            empty_df = pd.DataFrame()
                            concat_df = pd.concat([empty_df, mean_gene_profile], axis = 1).T
                            self.profiling_target_calls[slide] = pd.concat([self.profiling_target_calls[slide], concat_df], axis = 0)
                        except KeyError:
                            break

                        # Target call observations

                        quart_dict = {}
                        target_quart_list = [10, 25, 50, 75, 90, 100]
                        if df_dict:
                            for key in df_dict:
                                if not df_dict[key].empty:
                                    quart_dict[key] = np.percentile(df_dict[key].target_call_observations, target_quart_list)
                                    data_load_df[slide].at[fov, 'Target_call_obs_mode_{}'.format(key)] = mode(df_dict[key]['target_call_observations'])
                            for key in quart_dict:
                                for array_element in range(len(target_quart_list)):
                                    data_load_df[slide].at[fov, 'Target_call_obs_Q{}_{}'.format(str(target_quart_list[array_element]), key)] = quart_dict[key][array_element]
                            if not true_df.empty:
                                    if 'CellId' in true_df:
                                        true_df_cells_only = true_df.loc[~true_df['CellId'] != 0]
                                        diversity_df = true_df_cells_only.groupby(['CellId', 'target']).size().reset_index(name='Freq')
                                        per_cell_diversity = dict(Counter(diversity_df['CellId']))
                                        diversity_quarts = np.percentile(list(per_cell_diversity.values()), target_quart_list)
                                        for array_element in range(len(target_quart_list)):
                                            data_load_df[slide].at[fov, 'Unique_genes_per_cell_Q{}'.format(str(target_quart_list[array_element]))] = diversity_quarts[array_element]

                        # Per cell metrics 
                        try:
                            per_cell_dict = {}
                            for key in df_dict:
                                if 'CellId' in df_dict[key]:
                                    per_cell_dict[key] = dict(Counter(df_dict[key]['CellId']))
                        except KeyError:
                            break                        
                        if per_cell_dict:
                            for key in per_cell_dict:
                                per_cell_quart_dict = {}
                                per_cell_sum = sum(list(per_cell_dict[key].values()))
                                if 0 in list(per_cell_dict[key].keys()):
                                    outside_cell = per_cell_dict[key][0]
                                    data_load_df[slide].at[fov, 'Extracellular_{}'.format(key)] = outside_cell
                                    del per_cell_dict[key][0]
                                inside_cell_sum = sum(list(per_cell_dict[key].values()))
                                # number_of_cells_with_transcripts = len(per_cell_dict)
                                data_load_df[slide].at[fov, 'Intracellular_{}'.format(key)] = inside_cell_sum
                                if per_cell_sum > 0:
                                    data_load_df[slide].at[fov, 'Percent_intracellular_{}'.format(key)] = (inside_cell_sum / per_cell_sum) * 100
                                if slide in self.cellsumm_perslide:
                                    if not self.cellsumm_perslide[slide].empty:
                                        try:
                                            total_cells = self.cellsumm_perslide[slide]['Num_cells'][run + '_' + slide + '_' + fov]
                                            if total_cells > 0:
                                                per_cell_list = list(set(per_cell_dict[key].keys()))
                                                percent_cells_without_counts = (1 - (len(per_cell_list) / total_cells)) * 100
                                                data_load_df[slide].at[fov, 'Per_cell_mean_{}'.format(key)] = inside_cell_sum /  total_cells
                                                data_load_df[slide].at[fov, 'Percent_cells_without_any_{}'.format(key)] = percent_cells_without_counts
                                                try:
                                                    per_cell_quart_dict[key] = np.percentile(list(per_cell_dict[key].values()), target_quart_list)
                                                    data_load_df[slide].at[fov, 'Per_cell_mode_{}'.format(key)] = mode(list(per_cell_dict[key].values()))
                                                    for key in quart_dict:
                                                        for array_element in range(len(target_quart_list)):
                                                            data_load_df[slide].at[fov, 'Per_cell_Q{}_{}'.format(str(target_quart_list[array_element]), key)] = per_cell_quart_dict[key][array_element]
                                                except IndexError:
                                                    pass
                                        except KeyError:
                                            pass

                        # Use dict counter to get total counts
                        z_count = dict(Counter(target_calls_df['z']))
                        all_count = dict(Counter(target_calls_df['target']))
                        false_count = dict(filter(lambda item: 'False' in item[0], all_count.items()))
                        neg_count = dict(filter(lambda item: 'NegPrb' in item[0], all_count.items()))
                        
                        all_count_sum = sum(list(all_count.values()))
                        false_count_sum = sum(list(false_count.values()))
                        neg_count_sum = sum(list(neg_count.values()))
                        
                        non_target_list = list(neg_count.keys()) + list(false_count.keys())
                        gene_count = {k: v for k, v in all_count.items() if k not in non_target_list} 
                        gene_count_sum = sum(list(gene_count.values()))
                        gene_mean = np.mean(list(gene_count.values()))
                        
                        z_list = list(set(z_count.keys()))
                        z_list.sort()
                        if z_list:
                            data_load_df[slide].at[fov, 'Z_step_calls_range_max_minus_min'] = z_list[len(z_list) - 1] - z_list[0]
                            data_load_df[slide].at[fov, 'Z_step_calls_mode'] = mode(target_calls_df['z'])
                            if len(target_calls_df) > 1:
                                data_load_df[slide].at[fov, 'Z_step_calls_sd'] = statistics.stdev(target_calls_df['z'])

                        data_load_df[slide].at[fov, 'Total_transcripts'] = all_count_sum
                        data_load_df[slide].at[fov, 'Total_true_gene_transcripts'] = gene_count_sum
                        data_load_df[slide].at[fov, 'Total_false_codes'] = false_count_sum
                        data_load_df[slide].at[fov, 'Total_neg_probes'] = neg_count_sum

                        # Check LOD QC and record fails in self.warnings
                        # Check if there is enough false codes to run LOD statistics
                        if len(list(false_count.values())) > 1:
                            false_mean = np.mean(list(false_count.values()))
                            # Calculate 3 sigma for false codes
                            false_lod = round(((np.std(list(false_count.values()), ddof=1))*3) + false_mean, 4)
                            genes_above_false_lod = {k: v for k, v in gene_count.items() if v > false_lod}
                            data_load_df[slide].at[fov, 'FalseCodes_LOD'] = false_lod
                            data_load_df[slide].at[fov, 'Calls_above_False_LOD'] = len(genes_above_false_lod)
                            if gene_mean > 0:
                                data_load_df[slide].at[fov, 'Match_error_percent'] = (false_mean/gene_mean) * 100

                        # Check if there is enough neg probes to run LOD statistics
                        if len(list(neg_count.values())) > 1: 
                        # Run neg probe LOD statistics
                            neg_mean = np.mean(list(neg_count.values()))
                            # Calculate 3 sigma for neg codes
                            neg_lod = round(((np.std(list(neg_count.values()), ddof=1))*3) + neg_mean, 4)
                            genes_above_neg_lod = {k: v for k, v in gene_count.items() if v > neg_lod} 
                            data_load_df[slide].at[fov, 'NegPrbs_LOD'] = neg_lod 
                            data_load_df[slide].at[fov, 'Calls_above_Neg_LOD'] = len(genes_above_neg_lod)
                            if gene_mean > 0:
                                data_load_df[slide].at[fov, 'Exp_error_percent'] = (neg_mean/gene_mean) * 100

                    match_eff_list = self.labeled.paths_df[run][slide]['match_eff'].loc[~pd.isnull(self.labeled.paths_df[run][slide]['match_eff'])]
                    for fov in match_eff_list.index:
                        match_stats = ParseFile(match_eff_list[fov], fov_label = fov)
                        match_summ[slide] = pd.concat([match_summ[slide], match_stats.parsed])                                
                        try:
                            data_load_df[slide].at[fov, 'Match_eff_percent'] = match_summ[slide]['Matching_efficiency'][fov]   
                        except KeyError:
                            continue

                self.master_fov_df, self.master_slide_df = scrape_to_summary(data_load_df, self.master_fov_df, self.master_slide_df, run)

                for slide in self.profiling_target_calls:
                    self.profiling_target_calls[slide]['Origin_path'] = origin_path
                    self.profiling_target_calls[slide]['Exp_start_date'] = self.timestamp_readable
                    path_col = self.profiling_target_calls[slide].pop('Origin_path')
                    exp_start_col = self.profiling_target_calls[slide].pop('Exp_start_date')
                    self.profiling_target_calls[slide].insert(0, 'Exp_start_date', exp_start_col)
                    self.profiling_target_calls[slide].insert(0, 'Origin_path', path_col)
                    # Insert the index run label as a new column in the first column position
                    self.profiling_target_calls[slide].insert(0, 'Run_slide_fov', self.profiling_target_calls[slide].index)
                if len(self.profiling_target_calls) > 2:
                    ToExcel(self.profiling_target_calls, 'K:\\Gene_obs\\{}_gene_obs.xlsx'.format(run_label), multi_sheet = True)
        return

    def scrape_spatial_bc(self):
        '''Itarate over spatial bc metrics files and output multiple DataFrames and graphs'''
        if self.labeled.spatial:
            print('\nScraping spatialBC metrics file...')
            
            # Init dictionaries for holding incoming data and graphing info
            fid_summ = {}
            spot_input = {}
            raw_spot = {}
            raw_spot_slope_graph = {}
            raw_spot_sum = {}
            break_flag = False
            for run in self.labeled.spatial:
                if self.labeled.spatial[run]:
                    data_load_df = {}
                    for slide in self.labeled.spatial[run]:
                        data_load_df[slide] = pd.DataFrame()
                        raw_spot_sum[slide] = {}
                        raw_spot_slope_graph[slide] = pd.DataFrame()
                        complete_spatial = pd.DataFrame()
                        for table_num in range(len(self.labeled.spatial[run][slide])):
                            try:
                                spatial_input = pd.read_csv(self.labeled.spatial[run][slide][table_num])
                                if self.timestamp_readable is None:
                                    timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(self.labeled.spatial[run][slide][table_num]))
                                    self.timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")                            
                            except UnicodeDecodeError:
                                spatial_input = pd.read_excel(self.labeled.spatial[run][slide][table_num])
                            complete_spatial = pd.concat([complete_spatial, spatial_input])
                        spatial_input = complete_spatial.reset_index(drop = True)
                        for row in range(len(spatial_input)):
                            spatial_input.at[row, 'Spot_sum'] = spatial_input['BB'][row] + spatial_input['GG'][row] + spatial_input['YY'][row] + spatial_input['RR'][row]
                        try:
                            spatial_input = spatial_input.sort_values(by = ['Cycle', 'Pool', 'FOV', 'Spot', 'Z', 'Spot_sum'])
                        except:
                            break
                        spatial_input = spatial_input.drop_duplicates(subset = ['Cycle', 'Pool', 'FOV', 'Spot', 'Z'], keep = 'last')
                        try:
                            spot_input[slide] = spatial_input[['Cycle','Spot', 'Reporter', 'FOV', 'Z', 'BB', 'GG', 'YY', 'RR']]
                            fid_summ[slide] = spatial_input[['FOV', 'Fid', 'B_fid_bkg', 'G_fid_bkg', 'Y_fid_bkg', 'R_fid_bkg']]
                        # Legacy file format \\smi07\E\Run1112_20210413_dash07_ZS-TEST that doesn't align (just skip this)
                        except KeyError:
                            break_flag = True
                            break
                        fid_summ[slide] = fid_summ[slide].drop_duplicates(subset = 'FOV', keep = 'first')
                        fid_summ[slide] = fid_summ[slide].set_index('FOV')
                        data_load_df[slide]['Fid'] = fid_summ[slide]['Fid']
                        data_load_df[slide]['B_fid_bkg'] = fid_summ[slide]['B_fid_bkg']
                        data_load_df[slide]['G_fid_bkg'] = fid_summ[slide]['G_fid_bkg']
                        data_load_df[slide]['Y_fid_bkg'] = fid_summ[slide]['Y_fid_bkg']
                        data_load_df[slide]['R_fid_bkg'] = fid_summ[slide]['R_fid_bkg']


                        fov_list = list(set(spot_input[slide]['FOV']))
                        cycle_list = list(set(spot_input[slide]['Cycle']))
                        rep_list = list(set(spot_input[slide]['Reporter']))
                        spot_list = list(set(spot_input[slide]['Spot']))

                        hold_list = list()
                        for spot_num in spot_list:
                            if not (spot_num % 2) == 0:
                                hold_list.append(spot_num)
                        spot_list = hold_list

                        cycle_spot_sum = pd.DataFrame()
                        color_spot_sum = {'BB' : 0, 'GG' : 0, 'YY' : 0, 'RR' : 0}
                        color_list = ['BB', 'GG', 'YY', 'RR']
                        for fov in fov_list:
                            for cycle in cycle_list:
                                if cycle > 10:
                                    break
                                rep_cycle_image_df = spot_input[slide].loc[(spot_input[slide]['FOV'] == fov) & (spot_input[slide]['Cycle'] == cycle)]
                                rep_cycle_image_df = rep_cycle_image_df[rep_cycle_image_df['Spot'].isin(spot_list)]
                                cycle_spot_sum.at[fov, 'Total_raw_spots_for_C' + str(cycle)] = int(rep_cycle_image_df['BB'].sum() + rep_cycle_image_df['GG'].sum() + rep_cycle_image_df['YY'].sum() + rep_cycle_image_df['RR'].sum())
                                for color in color_list:
                                    color_spot_sum[color] = color_spot_sum[color] + rep_cycle_image_df[color].sum()
                            if self.instrument is not None:
                                instrument = self.instrument[:-2]
                                for color in color_list:
                                    if instrument == 'ALPHA':
                                        color_spot_sum[color] = (color_spot_sum[color] / len(cycle_list)) / 570520
                                    elif instrument == 'BETA':
                                        color_spot_sum[color] = (color_spot_sum[color] / len(cycle_list)) / 260835
                                    elif instrument == 'DASH':
                                        color_spot_sum[color] = (color_spot_sum[color] / len(cycle_list)) / 557133
                                    data_load_df[slide].at[fov, 'Raw_spots_per_square_um_per_cycle_{}'.format(color)] = color_spot_sum[color]

                        data_load_df[slide] = pd.concat([data_load_df[slide], cycle_spot_sum], axis = 1)
                        new_fov_index = list()
                        for index in data_load_df[slide].index:
                            if index > 99:
                                new_fov_index.append('F' + str(index))
                            elif index < 10:
                                new_fov_index.append('F00' + str(index))
                            else:
                                new_fov_index.append('F0' + str(index))
                        data_load_df[slide].index = new_fov_index

                        for fov in data_load_df[slide].index:
                            try:
                                if data_load_df[slide]['Total_raw_spots_for_C1'][fov] > 0:
                                    data_load_df[slide].at[fov, 'Percent_retained_spot_counts_C2overC1'] =  round((data_load_df[slide]['Total_raw_spots_for_C2'][fov] / data_load_df[slide]['Total_raw_spots_for_C1'][fov]) * 100, 2)
                            except KeyError:
                                pass
                            try:
                                if data_load_df[slide]['Total_raw_spots_for_C1'][fov] > 0:
                                    data_load_df[slide].at[fov, 'Percent_retained_spot_counts_C4overC1'] =  round((data_load_df[slide]['Total_raw_spots_for_C4'][fov] / data_load_df[slide]['Total_raw_spots_for_C1'][fov]) * 100, 2)
                                    data_load_df[slide].at[fov, 'Percent_retained_spot_counts_C8overC1'] =  round((data_load_df[slide]['Total_raw_spots_for_C8'][fov] / data_load_df[slide]['Total_raw_spots_for_C1'][fov]) * 100, 2)
                            except KeyError:
                                try:
                                    if data_load_df[slide]['Total_raw_spots_for_C1'][fov] > 0:
                                        data_load_df[slide].at[fov, 'Percent_retained_spot_counts_C{}overC1'.format(len(cycle_list))] =  round((data_load_df[slide]['Total_raw_spots_for_C{}'.format(len(cycle_list))][fov] / data_load_df[slide]['Total_raw_spots_for_C1'][fov]) * 100, 2)
                                except KeyError:
                                    pass

                        # Identify plexity
                        if 'Plexity' not in self.master_slide_df: 
                            if rep_list:
                                if len(rep_list) <= 8:
                                    self.master_slide_df.at[run + '_' + slide, 'Plexity'] = 100
                                    for fov in new_fov_index:
                                        if slide not in self.master_fov_df:
                                            self.master_fov_df[slide] = pd.DataFrame()
                                        if slide in self.master_fov_df:
                                            self.master_fov_df[slide].at[run + '_' + slide + '_' + fov, 'Plexity'] = 100
                                elif len(rep_list) > 8:
                                    self.master_slide_df.at[run + '_' + slide, 'Plexity'] = 1000
                                    for fov in new_fov_index:
                                        if slide in self.master_fov_df:
                                            self.master_fov_df[slide].at[run + '_' + slide + '_' + fov, 'Plexity'] = 1000

                self.master_fov_df, self.master_slide_df = scrape_to_summary(data_load_df, self.master_fov_df, self.master_slide_df, run)

        return

    def scrape_spot_zstep(self):
        z_range_re = re.compile(r'(Z-range: )(.*)(\n)')
        if self.labeled.paths_df:
            print('\nProcessing zstep data from spotfilelist...')
            # Make some dictionaries to hold all values from each FOV per category to make quick FOV comparisons        
            data_load_df = {}
            z_stats = {}
            z_range_list = list()
            sm_z_step = list()
            int_sm_z_step = list()
            for run in self.labeled.paths_df:
                for slide in self.labeled.paths_df[run]:
                    z_stats[slide] = pd.DataFrame()
                    data_load_df[slide] = pd.DataFrame()
                    z_list = self.labeled.paths_df[run][slide]['spot_file'].loc[~pd.isnull(self.labeled.paths_df[run][slide]['spot_file'])]
                    for fov in z_list.index:
                        with open(z_list[fov], 'r') as file_object:
                            line = file_object.readline()
                            while line:
                                z_range = z_range_re.search(line)
                                z_range_list.append(z_range.group(2))
                                line = file_object.readline()

                        reorg_z_range = [step.split(' ,') for step in z_range_list] 
                        int_sm_z_step = [int(element[0]) for element in reorg_z_range]

                        in_frame = [step for step in int_sm_z_step if -2 < step < 2]
                        if len(int_sm_z_step) > 0:
                            percent_in_frame = (len(in_frame) / len(int_sm_z_step)) * 100

                        zstep_sd = statistics.pstdev(int_sm_z_step)

                        data_load_df[slide].at[fov, 'Z1_step_percent_cycle_imgs_in_optimal_frame'] = percent_in_frame
                        data_load_df[slide].at[fov, 'Z1_step_standard_deviation'] = zstep_sd

                self.master_fov_df, self.master_slide_df = scrape_to_summary(data_load_df, self.master_fov_df, self.master_slide_df, run)
            
        return

    def scrape_plex(self):
        '''Scrape experiment information from config file to append to master dataframe'''
        # Check if there are any config file paths
        if self.labeled.plex:
            print('\n\nProcessing analysis_params file...')
            # Init recieving dictionary and instrument label
            data_load_df = {}
            for run in self.labeled.plex:
                for slide in self.labeled.plex[run]:
                    data_load_df[slide] = pd.DataFrame()    
                    fov = 'F001' # Dummy value so ParseFile can be used
                    # Parse config txt and convert to dataframe
                    plex_stats = ParseFile(self.labeled.plex[run][slide], fov_label = fov, plex = True)
                    data_load_df[slide] = pd.concat([data_load_df[slide], plex_stats.plex])

                # Load scraped values into master dataframes
                self.master_fov_df, self.master_slide_df = scrape_to_summary(data_load_df, self.master_fov_df, self.master_slide_df, run, config = True)
                
        return

class ScrapeProteinStats:
    '''Scrape various values from run output into more accesible readout'''
    def __init__(self, paths, run_label, origin_path, directory_size):
        
        # Recall previous global variables to check if database tables exist
        global new_fov_protein_table
        global new_slide_protein_table
        self.timestamp_readable = None

        print('\nGenerating summary files...')

        # Init master slide df and master fov df to summarize entire run, slides, and fov in a single table
        self.master_slide_df = pd.DataFrame()
        self.master_fov_df = {}
        self.profiling_target_calls = {}

        # Label files using path identifiers
        self.labeled = LabelFiles(file_paths = paths)
        # Process CellStatsDir
        self.master_fov_df, self.master_slide_df, self.cellsumm_perslide, self.timestamp_readable = scrape_cellstats(self.labeled, self.master_fov_df, self.master_slide_df, self.timestamp_readable)
        # Process config file
        self.master_fov_df, self.master_slide_df, self.instrument, self.timestamp_readable = scrape_config(self.labeled, self.master_fov_df, self.master_slide_df, self.timestamp_readable)
        # Process log file for run owner
        self.master_fov_df, self.master_slide_df, self.timestamp_readable = scrape_log(self.labeled, self.master_fov_df, self.master_slide_df, self.timestamp_readable)
        # Process ProteinDir
        self.scrape_protein_stats(self.timestamp_readable)

        column_constants_list = ['Instrument', 'SW_version', 'Protein_nCoder', 'Ch_HDR', 'Ch_thresh', 
        'Zsteps', 'Z_proj_method', 'DeltaZ', 'Bkg_sub_method', 'Decoding_method',
        'Ch_norm', 'Owner_1', 'Owner_2', 'Owner_3']

        # Get directory creation date
        if self.timestamp_readable is None:
            timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(origin_path))
            self.timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")
        # Convert directory size to Gb
        dir_size = round(directory_size / 1000000000, 2)

        # Modify origin path forward slashes because SQL doesn't like this character
        # Add origin path as a new column in per slide table
        origin_path = origin_path.replace('\\', '&')   
        if self.master_fov_df:
            for slide in self.master_fov_df:
                if self.master_fov_df[slide].empty:
                    self.master_fov_df[slide].index = [str(run_label) + slide + '_empty']
                self.master_fov_df[slide].insert(0, 'Run_directory_size_gb', dir_size)
                self.master_fov_df[slide].insert(0, 'Exp_start_date', self.timestamp_readable)
                self.master_fov_df[slide].insert(0, 'Origin_path', origin_path)
                self.master_fov_df[slide].insert(0, 'Run_slide_fov', self.master_fov_df[slide].index)
                self.master_fov_df[slide] = self.master_fov_df[slide].round(2)

            for col in column_constants_list:
                if col in self.master_slide_df:
                    col_index = self.master_slide_df.columns.get_loc(col)
                    for slide in self.master_fov_df:
                        self.master_fov_df[slide][col] = self.master_slide_df.iloc[0, col_index]

        # Send per fov table to SQL
            for slide in self.master_fov_df:
                fov_tb = ToSQL(self.master_fov_df[slide], 'all_fov_protein_v2', str(run_label) + '_' + slide, new_table = new_fov_protein_table, id_columns = ['Run_slide_fov', 'Origin_path'])
        try:
            # If new table was created flip the global new table variable
            if fov_tb.created == True:
                new_fov_protein_table = False
        except NameError:
            pass

        if self.master_slide_df.empty:
            self.master_slide_df.index = [str(run_label) + '_S1_empty']
        # Insert the index run label as a new column in the first column position and orgin path as a new column
        self.master_slide_df.insert(0, 'Run_directory_size_gb', dir_size)
        self.master_slide_df.insert(0, 'Exp_start_date', self.timestamp_readable)
        self.master_slide_df.insert(0, 'Origin_path', origin_path)
        self.master_slide_df.insert(0, 'Run_slide', self.master_slide_df.index)
        self.master_slide_df = self.master_slide_df.round(2)

        for col in column_constants_list:
            if col in self.master_slide_df:
                col_index = self.master_slide_df.columns.get_loc(col)
                col_series = self.master_slide_df.iloc[:, col_index]
                col_series_value = list(set(col_series.dropna()))
                self.master_slide_df[col] = col_series_value[0]

        # Send per slide dataframe to SQL
        slide_tb = ToSQL(self.master_slide_df, 'all_slide_protein_v2', str(run_label) + '_per_fov_df', new_table = new_slide_protein_table, id_columns = ['Run_slide', 'Origin_path'])
        try:
        # If new table was created flip the global new table variable
            if slide_tb.created == True:
                new_slide_protein_table = False
        except NameError:
            pass

    def scrape_protein_stats(self, timestamp_readable):
        '''Itarate over spatial bc metrics files and output multiple DataFrames and graphs'''
        if self.labeled.protein_paths:
            print('\nScraping protein metrics file...')
            
            # Init dictionaries for holding incoming data and graphing info
            for run in self.labeled.protein_paths:
                if self.labeled.protein_paths[run]:
                    data_load_df = {}
                    protein_df = {}
                    for slide in self.labeled.protein_paths[run]:
                        data_load_df[slide] = pd.DataFrame()
                        protein_df[slide] = pd.DataFrame()
                        for col in list(self.labeled.protein_paths[run][slide].columns):
                            for fov in self.labeled.protein_paths[run][slide].index:
                                try:
                                    protein_input = pd.read_csv(self.labeled.protein_paths[run][slide][col][fov])
                                    if self.timestamp_readable is None:
                                        timestamp_ctime_format = dt.datetime.fromtimestamp(os.path.getctime(self.labeled.protein_paths[run][slide][col][fov]))
                                        self.timestamp_readable = timestamp_ctime_format.strftime("%Y %m %d, %H:%M")
                                except ValueError:
                                    continue

                                data_load_df[slide].at[fov, col + '_per_cell_mean_counts'] = protein_input['Avg'].mean()
                                data_load_df[slide].at[fov, col + '_per_cell_total_counts'] = protein_input['Sum'].mean()
                                data_load_df[slide].at[fov, col + '_per_fov_total_counts'] = protein_input['Sum'].sum()

                                if self.instrument is not None:
                                    instrument = self.instrument[:-2]
                                    if instrument == 'ALPHA':
                                        data_load_df[slide].at[fov, col + '_total_counts_per_um_squared'] = protein_input['Sum'].sum() / 570520
                                    elif instrument == 'BETA':
                                        data_load_df[slide].at[fov, col + '_total_counts_per_um_squared'] = protein_input['Sum'].sum() / 260835
                                    elif instrument == 'DASH':
                                        data_load_df[slide].at[fov, col + '_total_counts_per_um_squared'] = protein_input['Sum'].sum() / 557133

                    self.master_fov_df, self.master_slide_df = scrape_to_summary(data_load_df, self.master_fov_df, self.master_slide_df, run)

        return 

class FindRuns:
    '''Generates a list of runs from a work station'''
    def __init__(self, compiled_transcript, compiled_protein, network, drive):
        self.network = network
        self.drive = drive
        self.run_paths = self.__find_files(compiled_transcript, compiled_protein)
    def __find_files(self, compiled_transcript, compiled_protein):
        # Recieve previously analyzed runs from SQL server and add some keywords to exclude when walking drive directories
        compiled_transcript.extend(['miniconda', 'Sim', 'Run4468_20211001','c drive', 'CDrive_Backups', 'tertiary', 'c drive backup', 'cellMatrix', 'RECYCLE.BIN', '0000', 'Installer', 'Program Files', 'Run1000', 'AppData', 'SoftwareDistribution', 'ProgramData', 'Package Cache', 'Windows'])
        # Add previously compiled protien directories
        compiled_transcript.extend(compiled_protein)
        str_compiled_list = [str(item) for item in compiled_transcript]
        # Transform compiled list into a regex compatible list of matching strings
        compiled_re_list = ('|').join(str_compiled_list)
        compiled_re_fun = re.compile(compiled_re_list)

        # Generate a run regex for various run label forms
        run_regex = re.compile(r'Run\d{4}A|Run\d{4}|RunA\d{4}|RunB\d{4}|A\d{4}|R\d{4}')

        paths = {}
        # Try various password for workstation connections
        try:
            print('\n\nTrying to connect to ---' + '\\\\' + self.network + '\\' + self.drive + '---')
            wnet_connect('{}'.format(self.network), 'project', 'charlie13')
        except:
            print('\n\n**WARNING** Unable to connect to ---' + '\\\\' + self.network + '\\' + self.drive + '---')
            try:
                print('\n\n Trying alternative login credentials for ---' + '\\\\' + self.network + '\\' + self.drive + '---')
                wnet_connect('{}'.format(self.network), 'Nsadmin', 'BetaBetaBeta!')
            except:
                print('\n\n**WARNING** Alternate login still fails connection for ---' + '\\\\' + self.network + '\\' + self.drive + '---')
        # Walk directories in each found drive
        if os.path.exists('\\\\' + self.network + '\\' + self.drive):
            print('\n\n*Connection success* \nCrawling through directories for network drive ---{}---\n'.format(self.network + '\\' + self.drive))
            for root, dirs, files in os.walk('\\\\' + self.network + '\\' + self.drive, topdown=True):
                run = run_regex.search(root)
                exclude_str = root.replace('\\', '&')
                exclude = compiled_re_fun.search(exclude_str)
                if exclude is not None:
                    print('----Excluding directory----' + root)
                    # If directory is marked for exclusion ignore all sub-directories
                    dirs[:] = []
                    continue
                if run is not None:
                    paths[root] = run.group()
                    print(root)
                    # If directory is marked for run analysis ignore all sub-directories
                    dirs[:] = []

        return paths

if __name__ == "__main__":

    # List of all available networks
    network_list = [ 
    r'beta04',
    r'smi08',  
    r'alpha02', r'alpha01', r'alpha03', r'alpha04', r'alpha06',
    r'alpha07', r'alpha08', 
    r'alpha09', 
    r'alpha10', 
    r'dash02', 
    r'dash03', r'dash04', 
    r'dash05', 
    r'dash08', 
    r'dash10', r'smi01', r'smi03', r'smi04', r'smi05', 
    r'smi06',  
    r'smi07',  r'smi09',  r'smi10',  
    r'dashx01',  r'dashx08',  r'beta01',  r'beta02',  r'beta03',
    r'beta05',  r'beta06',  r'beta07',  r'beta08',  r'beta09',  r'beta10',  r'beta11',  r'beta12',  r'beta13',
    r'beta14',  r'beta15',  r'beta16',  r'beta17', r'alpha05', r'dash06', r'dash09', r'smi02',
     r'dash07', r'dash01'
     ]    
    # List of likely drives for each work station
    drive_list = ['D', 'E', 'F', 'G', 'H', 'I', 'J','K','L','M','N']

    # Init var to flag for a protein run
    global protein_check
    # Get previous runs from SQL server
    run_check = FromSQL()
    # Iterate through all networks and drives
    for network in network_list:
        for drive in drive_list:
            # Find all drive runs using keyword identifiers
            paths = FindRuns(run_check.compiled_transcript, run_check.compiled_protein, network, drive)
            for path in paths.run_paths:
                print('\nCompiling all file paths for *****{}*****'.format(paths.run_paths[path]))
                # Compile file paths for each run directory
                all_files = FindFiles(input_directory = path)
                # Scrape for protein information or transcript information
                if protein_check == True:
                    ScrapeProteinStats(paths = all_files.paths, run_label = paths.run_paths[path], origin_path = path, directory_size = all_files.directory_size)
                elif protein_check == False:
                    ScrapeTranscriptStats(paths = all_files.paths, run_label = paths.run_paths[path], origin_path = path, directory_size = all_files.directory_size)

#TODO: 
# Add a flag for every file type that marks where there are duplicate file types and which one was chosen






from cmath import nan
import pandas as pd
import re
import numpy as np
import pathlib
import gc
import pandas as pd
import numpy as np
import os
from collections import Counter
import math
import xlsxwriter
import time
import io
import string
import os
import datetime
from scipy import stats
import plotly.express as px
import itertools

def sort_columns(df, format, additional_col = None):
    '''Remove metadata columns and sort remaining data columns alphabetically'''

    remove_list = ['Ch_HDR', 'Ch_norm', 'Ch_thresh', 'DeltaZ', 'Decoding_method',	'Owner_3',	'Owner_2',
    'Bkg_sub_method', 'Zsteps', 'Voting_version','SW_version', 'Instrument_type', 'Instrument', 'Owner_1', 'Species', 'Storage', 
    'Tissue_type', 'Run_directory_size_gb', 'Plexity', 'Exp_start_date', 'Origin_path', format]
    col_reorder_dict = {}
    for item in remove_list:
        col_reorder_dict[item] = df[item]
    df = df.drop(remove_list, axis = 1)
    df = df.reindex(sorted(df.columns), axis=1)
    for item in remove_list:
        df.insert(0, item, col_reorder_dict[item])
    
    return df

class CleanTable():
    '''Cleans empty entries, formats select columns, and resolves ID collisions'''
    def __init__(self, input_db, format):
        # Load in dataframes
        self.format = format
        df = pd.read_csv(input_db)
        df = self.clean_extra_entries(df, format)
        df = self.process(df)
        df = self.merge_dupes(df)
        self.df = self.scrape_data(df)

        df = self.scrape_data(df)

        if format == 'Run_slide':
            save_label = 'slide'
        elif format == 'Run_slide_fov':
            save_label = 'fov'
        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Transcript_{}_db_v5.xlsx'.format(save_label), engine='xlsxwriter')
        self.df.to_excel(writer,  sheet_name = 'Transcript_{}'.format(format))
        writer.save()       

    def clean_extra_entries(self, df, format):
        '''Removes extra erroneous cycle spot sum collumns'''
        drop_list = list()
        for cycle_num in range(11, 80):
            drop_list.append('Total_raw_spots_for_C{}'.format(cycle_num))
            if format == 'Run_slide':
                drop_list.append('Total_raw_spots_for_C{}_sd'.format(cycle_num))
        if format == 'Run_slide':
            drop_list.append('Z_step_calls_sd_sd')
        dropped_df = df.drop(drop_list, axis = 1)

        return dropped_df

    def process(self, df):
        '''Main post-processing functions'''
        # Drop empties
        for row in range(len(df)):
            if 'empty' in df[self.format][row]:
                df = df.drop(row)
        df = df.reset_index(drop = True)
        # Replace nans with 0
        df = df.replace(nan, 0)

        # Correctly annotates some 1000 plex entries based on LOD metrics
        # Changes all run owner names to upper case
        new_path_list = list()
        upper_owner1_list = list()
        upper_owner2_list = list()
        upper_owner3_list = list()
        upper_owner4_list = list()
        for row in range(len(df)):
            if df['Calls_above_False_LOD'][row] > 130 or df['Calls_above_Neg_LOD'][row] > 130 or  df['Unique_genes_per_cell_Q100'][row] > 130:
                df.at[row, 'Plexity'] = 1000
            new_path_list.append(df['Origin_path'][row].replace('&', '\\'))
            if isinstance(df['Owner_1'][row], str):
                upper_owner1_list.append(df['Owner_1'][row].upper())
            else:
                upper_owner1_list.append((df['Owner_1'][row]))
            if isinstance(df['Owner_2'][row], str):
                upper_owner2_list.append(df['Owner_2'][row].upper())
            else:
                upper_owner2_list.append((df['Owner_1'][row]))
            if isinstance(df['Owner_3'][row], str):
                upper_owner3_list.append(df['Owner_3'][row].upper())
            else:
                upper_owner3_list.append((df['Owner_1'][row]))
            if format == 'Run_slide':
                if isinstance(df['Owner_4'][row], str):
                    upper_owner4_list.append(df['Owner_4'][row].upper())
                else:
                    upper_owner4_list.append((df['Owner_1'][row]))

        df['Owner_1'] = upper_owner1_list
        df['Owner_2'] = upper_owner2_list
        df['Owner_3'] = upper_owner3_list
        if format == 'Run_slide':
            df['Owner_4'] = upper_owner4_list
        df['Origin_path'] = new_path_list
        
        # Drop protein entries
        for row in range(len(df)):
            if df['Protein_nCoder'][row] != 0:
                df = df.drop(row)
        df = df.drop('Protein_nCoder', axis = 1)

        remove_list = ['Ch_HDR', 'Ch_norm', 'Ch_thresh', 'DeltaZ', 'Decoding_method',	'Owner_2',	
        # 'Owner_3',
        'Bkg_sub_method', 'Zsteps', 'Voting_version', 'SW_version', 'Instrument', 'Owner_1', 'Run_directory_size_gb', 'Plexity', 'Exp_start_date', 'Origin_path', self.format]

        # Slide db has owner 4 but fov db does not
        if format == 'Run_slide':
            remove_list.append('Owner_4')

        # Drop any all zero rows
        just_data_df = df.drop(remove_list, axis = 1)
        drop_zeroes = just_data_df.loc[~(just_data_df==0).all(axis=1)]
        df = df[df.index.isin(drop_zeroes.index)]
        df = df.reset_index(drop = True)
        # Drop duplicates
        df = df.sort_values(by = [self.format, 'Run_directory_size_gb'], ascending = [True, True])
        df = df.drop_duplicates(subset=[self.format, 'Exp_start_date'], keep = 'last')
        df = df.reset_index(drop = True)

        col_reorder_dict = {}
        for item in remove_list:
            col_reorder_dict[item] = df[item]
        df = df.drop(remove_list, axis = 1)
        df = df.reindex(sorted(df.columns), axis=1)
        for item in remove_list:
            df.insert(0, item, col_reorder_dict[item])

        return df
            
    def merge_dupes(self, df):
        '''If entries are duplicated, merges relevant information into one entry'''
        df = df.set_index(self.format)
        results = df.groupby([self.format]).size()
        dupes = results[results == 2]

        all_merge_rows = pd.DataFrame()
        for index in dupes.index:
            single_row_merge = pd.DataFrame()
            comp_rows = df.loc[[index]]
            comp_rows = comp_rows.reset_index()
            for col in comp_rows.columns:
                try:
                    single_row_merge.at[index, col] = max(comp_rows[col][0], comp_rows[col][1])
                except TypeError:
                    if isinstance(comp_rows[col][0], str):
                        single_row_merge.at[index, col] = comp_rows[col][0]
                    elif isinstance(comp_rows[col][1], str):
                        single_row_merge.at[index, col] = comp_rows[col][1]
            if comp_rows['Run_directory_size_gb'][0] > comp_rows['Run_directory_size_gb'][1]:
                single_row_merge.at[index, 'Origin_path'] = comp_rows['Origin_path'][0]
            elif comp_rows['Run_directory_size_gb'][1] > comp_rows['Run_directory_size_gb'][0]:
                single_row_merge.at[index, 'Origin_path'] = comp_rows['Origin_path'][1]

            all_merge_rows = pd.concat([all_merge_rows, single_row_merge])

        df = df.drop(dupes.index)
        df[self.format] = df.index
        df = pd.concat([df, all_merge_rows])   

        df = df.sort_values(by = [self.format, 'Exp_start_date'])
        df = df.drop_duplicates(subset = [self.format], keep = 'last')

        return df

    def scrape_data(self, df):
        # Init regex patterns into a master dict
        regex_dict = {
            'Tissue_type' : r'(?i)3cpa|5cpa|9cpa|11cpa|(?<!16-)37cpa|3 cpa|9 cpa|11 cpa|37 cpa|16cpa|8cpa|brain|tma|breast|colon|tonsil|heart|liver|skin|pancreas|crc|melanoma|mel(?!anoma)|nsclc|cancerous liver|cancerousliver|lncap|lymph|kidney|neu',
            'Storage' : r'(?i)ffixd|ffpe|ff(?!pe)',
            'Species' : r'(?i)mouse|ms(?!tain)|human|hu(?!man)',

        }
        # Init collection list dict
        list_dict = {}

        instrument_type_list = list()
        # Loop over all rows to scrape information
        df = df.reset_index(drop = True)
        for row in range(len(df)):
            if df['Instrument'][row] != 0:
                instrument_type_list.append(df['Instrument'][row][:-2])
            else:
                instrument_type_list.append(0)

            for regex_target in regex_dict:
                row_match = re.findall(regex_dict[regex_target], (str(df.at[row,'Origin_path'])))
                if regex_target not in list_dict:
                    list_dict[regex_target] = list()
                if row_match:
                    if len(row_match) == 1:
                        if row_match[0].upper() == 'MS':
                             row_match[0] = 'MOUSE'
                        if row_match[0].upper() == 'HU':
                             row_match[0] = 'HUMAN'
                        if row_match[0].upper() == 'NEU':
                             row_match[0] = 'BRAIN'
                        if row_match[0].upper() == 'LYMPH':
                             row_match[0] = 'LYMPHNODE'
                        list_dict[regex_target].append(row_match[0].upper())
                    else:
                        list_dict[regex_target].append(0)
                else:
                    list_dict[regex_target].append(0)

        list_dict['Instrument_type'] = instrument_type_list

        for regex_target in list_dict:
            df.insert(7, regex_target, list_dict[regex_target])

        df = df.set_index(self.format)
        df.index.name = self.format

        return df

class MergeMeta():
    '''Retrieves metadata from various sources and merges the data along run, slide, and fov ids'''
    def __init__(self, slide_db_path, fov_db_path, ff_meta_path, ffpe_meta_path, master_meta_path):
        # Load in dataframes
        if isinstance(slide_db_path, str):
            slide_db = pd.read_excel(slide_db_path)
        else:
            slide_db = slide_db_path
        if isinstance(fov_db_path, str):
            fov_db = pd.read_excel(fov_db_path)
        else:
            fov_db = fov_db_path


        ff_meta = pd.read_excel(ff_meta_path)
        ffpe_meta = pd.read_excel(ffpe_meta_path)
        master_meta = pd.read_excel(master_meta_path)

        df_dict = {'ff_meta' : ff_meta, 'ffpe_meta' : ffpe_meta}
        for key in df_dict:
            df_dict[key] = self.create_id(df_dict[key])

        qb_merge, qb_merge_raw = self.merge_qb(slide_db, df_dict['ff_meta'], df_dict['ffpe_meta'])
        master_merge, master_merge_raw = self.merge_master(fov_db, master_meta)

        merged, slide_merge, fov_merge = self.merge_slide_and_fov_meta(qb_merge, master_merge)

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Merged with meta\\QuickBase_full_merge_v5.xlsx', engine='xlsxwriter')
        qb_merge_raw.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Merged with meta\\QuickBase_merge_v5.xlsx', engine='xlsxwriter')
        qb_merge.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Merged with meta\\CompiledSecondaryData_fov_full_merge_v5.xlsx', engine='xlsxwriter')
        master_merge_raw.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Merged with meta\\CompiledSecondaryData_fov_merge_v5.xlsx', engine='xlsxwriter')
        master_merge.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Merged with meta\\Complete_slide_meta_v5.xlsx', engine='xlsxwriter')
        slide_merge.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Merged with meta\\Complete_fov_meta_v5.xlsx', engine='xlsxwriter')
        fov_merge.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

    def create_id(self, df):
        run_slide_list = list()
        for row in range(len(df)):
            run_slide_list.append('Run' + str(df['Alternate Run Number'][row]) + '_' + str(df['Slide Number'][row]))
        df['Run_slide'] = run_slide_list

        for row in range(len(df)):
            if 'nan' in df['Run_slide'][row]:
                df = df.drop(row)

        df = df.reset_index(drop = True)

        return df

    def merge_qb(self, slide_db, ff_meta, ffpe_meta):
        '''Recieve metadata from QuickBase'''
        tissue_re_fun = re.compile(r'(?i)3cpa|5cpa|9cpa|11cpa|(?<!16-)37cpa|3 cpa|9 cpa|11 cpa|37 cpa|16cpa|8cpa|brain|tma|breast|colon|tonsil|heart|liver|skin|pancreas|crc|melanoma|mel(?!anoma)|nsclc|cancerous liver|cancerousliver|lncap|lymph|kidney|neu|bladder|lung|placenta|prostate|adenocarcinoma')
        species_re_fun = re.compile(r'(?i)neu|hu')

        df = pd.concat([ff_meta, ffpe_meta], ignore_index = True)

        instrument_list = list()
        species_list = list()
        tissue_list = list()
        for row in range(len(df)):
            tissue_collected_flag = 0
            if isinstance(df['Run - Related CosMx'][row], str):
                instrument_list.append(df['Run - Related CosMx'][row].replace('-', '').upper())
            else:
                instrument_list.append(0)
                
            df.at[row, 'Sample Type'] = df['Sample Type'][row].replace('Fresh Frozen', 'FF').upper()
            
            if isinstance(df['ISH Probe Pool'][row], str):
                species = species_re_fun.search(df['ISH Probe Pool'][row])
                if species is not None:
                    if species.group().upper() == 'NEU':
                        species_list.append('MOUSE')
                        tissue_list.append('BRAIN')
                        tissue_collected_flag = 1
                    elif species.group().upper() == 'HU':
                        species_list.append('HUMAN')
                else:
                    species_list.append(0)
            else:
                species_list.append(0)

            if tissue_collected_flag == 0:
                if isinstance(df['Tissue Type'][row], str):
                    tissue = tissue_re_fun.search(df['Tissue Type'][row])
                    if tissue is not None:
                        tissue_list.append(tissue.group().upper())
                    else:
                        tissue_list.append(0)
                else:
                    tissue_list.append(0)

        df['Run - Related CosMx'] = instrument_list
        df['Tissue_type'] = tissue_list
        df['Species'] = species_list

        trimmed_df = df[['Run_slide', 'Tissue_type', 'Run - Related CosMx', 'Sample Type', 'ISH Probe Pool', 'Species']]

        merged_trim = slide_db.merge(trimmed_df, on = 'Run_slide', how = 'left', suffixes = ('', '_right'))
        merged_full = slide_db.merge(df, on = 'Run_slide', how = 'left', suffixes = ('', '_right'))


        for row in range(len(merged_trim)):
            if merged_trim['Run - Related CosMx'][row] == merged_trim['Instrument'][row]:
                if merged_trim['Species'][row] == 0:
                    merged_trim.at[row, 'Species'] = merged_trim['Species_right'][row]
                if merged_trim['Storage'][row] == 0:
                    merged_trim.at[row, 'Storage'] = merged_trim['Sample Type'][row]
                if merged_trim['Tissue_type'][row] == 0:
                    merged_trim.at[row, 'Tissue_type'] = merged_trim['Tissue_type_right'][row]

        merged_trim = merged_trim.drop(['Run - Related CosMx',	'Sample Type', 'ISH Probe Pool', 'Tissue_type_right', 'Species_right'], axis = 1)

        # merged = merged.drop(['Tissue_type_right', 'Run - Related CosMx', 'Sample Type', 'Species_right'], axis = 1)

        merged_trim = merged_trim.reset_index(drop = True)

        return merged_trim, merged_full

    def merge_master(self, fov_db, master_meta):
        '''Receive metadata from chem annotations'''
        storage_re_fun = re.compile(r'(?i)_ffixd_|ffixd_|ffix_|ffpe_|ffpe-|ff_')
        species_re_fun = re.compile(r'(?i)ms_|human |mouse')

        storage_list = list()
        species_list = list()
        for row in range(len(master_meta)):
            if isinstance(master_meta['Tissue_type'][row], str):
                storage = storage_re_fun.search(master_meta['Tissue_type'][row])
                species = species_re_fun.search(master_meta['Tissue_type'][row])
                if species is not None:
                    if species.group() == 'Ms_' or species.group() == 'Mouse':
                        species_list.append('MOUSE')
                    if species.group() == 'Human ':
                        species_list.append('HUMAN')               
                    species_match = master_meta['Tissue_type'][row].replace(species.group(), '')
                    master_meta.at[row, 'Tissue_type'] = species_match
                else:
                    species_list.append(0)
                if storage is not None:
                    if storage.group() == '_Ffixd_':
                        storage_list.append('FFIXD')
                    elif storage.group() == 'ffix_':
                        storage_list.append('FFIXD')
                    elif storage.group() == 'ffix_':
                        storage_list.append('FFIXD')
                    else:
                        storage_list.append(storage.group().upper()[:-1])
                    tissue_type = master_meta['Tissue_type'][row].replace(storage.group(), '')
                    master_meta.at[row, 'Tissue_type'] = tissue_type
                else:
                    storage_list.append(0)
            else:
                storage_list.append(0)
                species_list.append(0)

        
        master_meta['Storage'] = storage_list
        master_meta['Species'] = species_list

        master_meta_trim = master_meta[['Tissue_type', 'Storage', 'Species', 'Run_slide_fov']]

        master_merge_full = fov_db.merge(master_meta, on = 'Run_slide_fov', how = 'left', suffixes = ('', '_right'))
        master_meta_trim = fov_db.merge(master_meta_trim, on = 'Run_slide_fov', how = 'left', suffixes = ('', '_right'))

        for row in range(len(master_meta_trim)):
            if master_meta_trim['Tissue_type'][row] == 0:
                if isinstance(master_meta_trim['Tissue_type_right'][row], str):
                    master_meta_trim.at[row, 'Tissue_type'] = master_meta_trim['Tissue_type_right'][row].upper()
            if master_meta_trim['Storage'][row] == 0:
                if isinstance(master_meta_trim['Storage_right'][row], str):
                    master_meta_trim.at[row, 'Storage'] = master_meta_trim['Storage_right'][row].upper()
            if master_meta_trim['Species'][row] == 0:
                if isinstance(master_meta_trim['Species_right'][row], str):
                    master_meta_trim.at[row, 'Species'] = master_meta_trim['Species_right'][row].upper()

        master_meta_trim = master_meta_trim.drop(['Tissue_type_right', 'Storage_right', 'Species_right'], axis = 1)

        return master_meta_trim, master_merge_full

    def merge_slide_and_fov_meta(self, slide, fov):
        '''Combine metadata from slide and fov merges'''
        meta_list = ['Tissue_type', 'Species', 'Storage']

        run_slide_list = list()
        for row in range(len(fov)):
            run_slide_list.append(fov['Run_slide_fov'][row][:-5])
        fov['Run_slide'] = run_slide_list

        fov_meta = fov[['Tissue_type', 'Species', 'Storage', 'Run_slide']]
        fov_meta = fov_meta.drop_duplicates(subset = 'Run_slide', keep = 'last')
        slide_meta = slide[['Tissue_type', 'Species', 'Storage', 'Run_slide']]
        merged = fov_meta.merge(slide_meta, on = 'Run_slide', suffixes = ('_sld', '_fov'))

        for item in meta_list:
            for row in range(len(merged)):
                if merged[item + '_sld'][row] != merged[item + '_fov'][row]:
                    if merged[item + '_sld'][row] == 0:
                        merged.at[row, item] = merged[item + '_fov'][row]
                    if merged[item + '_fov'][row] == 0:
                        merged.at[row, item] = merged[item + '_sld'][row]
                else:
                    merged.at[row, item] = merged[item + '_sld'][row]

        fov = fov.drop(['Tissue_type', 'Species', 'Storage'], axis = 1)
        slide = slide.drop(['Tissue_type', 'Species', 'Storage'], axis = 1)
        drop_merge = merged.drop(['Tissue_type_sld', 'Species_sld', 'Storage_sld', 'Tissue_type_fov', 'Species_fov', 'Storage_fov'], axis = 1)

        fov = fov.merge(drop_merge, how = 'left', on = 'Run_slide')
        slide = slide.merge(drop_merge, how = 'left',  on = 'Run_slide')

        fov = sort_columns(fov, format = 'Run_slide_fov')
        slide = sort_columns(slide, format = 'Run_slide')

        fov = fov.set_index('Run_slide_fov')
        slide = slide.set_index('Run_slide')
        merged = merged.reset_index(drop = True)

        return merged, slide, fov

class FindDifference():
    '''Compares current database with previous data analytics as a benchmarking safety check'''
    def __init__(self, merged):
        # Load in dataframes

        if isinstance(merged, str):
            merged = pd.read_excel(merged)

        diff_df, one_off, ten_off = self.find_difference(merged)

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Metadata\\CompiledSecondaryData_comparison_complete_v5.xlsx', engine='xlsxwriter')
        diff_df.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()        

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Metadata\\CompiledSecondaryData_comparison_one_off_v5.xlsx', engine='xlsxwriter')
        one_off.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()        

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Metadata\\CompiledSecondaryData_comparison_ten_off_v5.xlsx', engine='xlsxwriter')
        ten_off.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()        

    def find_difference(self, df):
        '''Subtract dataframes accross merged entries'''
        col_dict = {
        'NegPrbs_LOD' : 'NegPrbLOD',
        'Per_cell_mean_false_codes' : 'Total_False_Codes_Per_Cell',
        'Per_cell_mean_true_genes' : 'Total_True_Genes/Cell',
        'Per_cell_mean_neg_probes' : 'TotalNegPrb_PerCell',
        'Percent_intracellular_false_codes' : 'PercentFalseCodesInCells',
        'Percent_intracellular_neg_probes' : 'PercentNegPrbsInCells',
        'Percent_intracellular_true_genes' : 'PercentTrueGenesInCells',
        'Total_false_codes' : 'Total_False_Codes',
        'Total_neg_probes' : 'Total_Neg_Prb', 
        'Total_transcripts' : 'Total_Transcripts',
        'Total_true_gene_transcripts' : 'Total_True_Gene_Transcripts',
        'Num_cells' : 'Cell_Count'
        }

        df = df.sort_values(by = 'ID')
        df = df[df.ID.notna()]
        df = df.reset_index(drop = True)

        diff_df = pd.DataFrame()
        one_off_df = pd.DataFrame()
        ten_off_df = pd.DataFrame()

        for col in col_dict:
            for row in range(len(df)):
                difference = df[col][row] - df[col_dict[col]][row]
                diff_df.at[row, col] = difference
                if -1 < difference < 1:
                    one_off_df.at[row, col] = difference
                if -10 < difference < 10:
                    ten_off_df.at[row, col] = difference

        df_dict = {'total_diff' : diff_df, 'one_off' : one_off_df, 'ten_off' : ten_off_df}

        for df_subset in df_dict:
            col_to_insert = ['Tissue_type','Team', 'Origin_path', 'Run_slide_fov']
            for col in col_to_insert:
                df_dict[df_subset].insert(0, col, df[col])

            df_dict[df_subset] = df_dict[df_subset].sort_values(by = 'Run_slide_fov')

            df_dict[df_subset] = df_dict[df_subset].reset_index(drop = True)

        return df_dict['total_diff'], df_dict['one_off'], df_dict['ten_off']

class SummaryStats():
    '''Generates descriptive metadata stats for the entire database collection'''
    def __init__(self, slide, fov):
        if isinstance(slide, str):
            slide = pd.read_excel(slide)
        if isinstance(fov, str):
            fov = pd.read_excel(fov)

        slide, include_list = self.non_zero_slide(slide)
        fov = self.non_zero_fov(fov, include_list)

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Summary_stats\\Slide_table_filtered_v5.xlsx', engine='xlsxwriter')
        slide.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()            

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Summary_stats\\Fov_table_filtered_v5.xlsx', engine='xlsxwriter')
        fov.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()            

        self.val_dict = {}

        filtered = self.get_slide_metrics(slide)
        self.get_fov_metrics(fov)

        summary = pd.DataFrame()
        metric_list = [
        'Total_Cell_Number',
        'Total_Cell_Assigned_Transcripts',
        'Average_Cell_Number_per_Slide',
        'Average_Cells_per_FOV',
        'Average_#_FOVs_per_Slide',
        'Average_%_Pass_QC_per_Slide',
        'Average_Tx_per_um^3_per_Slide',
        'Average_Genes_Detected_per_Slide',
        'Average_Mean_Tx_per_Cell_per_Slide',
        'Average_Q90_Tx_per_Cell_per_Slide',
        'Number_of_Slides_Queried',
        'Total_#_FOVs_collected'
        ]

        for item in metric_list:
            summary.at[item, 'Values'] = self.val_dict[item]

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Summary_stats\\Summary_metrics_v5.xlsx', engine='xlsxwriter')
        summary.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()            

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Summary_stats\\Pass_QC_slides_v5.xlsx', engine='xlsxwriter')
        filtered.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()         

    def non_zero_slide(self, df):
        '''Ensure that entries are complete for select summary metrics'''
        non_zero_list = ['Num_cells', 'Extracellular_false_codes', 'Extracellular_neg_probes', 'Extracellular_true_genes', 
        'Intracellular_false_codes', 'Intracellular_neg_probes', 'Intracellular_true_genes', 'Per_cell_mean_false_codes', 'Per_cell_mean_true_genes',
        'Per_cell_mean_neg_probes']

        include_list = list()
        for row in range(len(df)):
            zero_present = False
            for col in non_zero_list:
                if df[col][row] == 0:
                    zero_present = True
                    break
            if zero_present == False:
                include_list.append(df['Run_slide'][row])

        df = df[df['Run_slide'].isin(include_list)]
        df = df[df['Plexity'] == 1000]
        df = df[df['Num_cells'] > 800]

        date_list = list()
        for date in df['Exp_start_date']:
            date_list.append(datetime.datetime.strptime(date, '%Y %m %d, %H:%M'))
        df['Exp_start_date'] = date_list

        df['Exp_start_date'] = pd.to_datetime(df['Exp_start_date'])
        df = df[(df['Exp_start_date'] > '2022-01-01 07:30:00')]

        df = df.reset_index(drop = True)

        include_list = df['Run_slide']

        return df, include_list

    def non_zero_fov(self, df, include_list):
        '''Ensure that entries are complete for select summary metrics'''
        run_slide_list = list()
        for row in range(len(df)):
            run_slide_list.append(df['Run_slide_fov'][row][:-5])
        df['Run_slide'] = run_slide_list
   
        df = df[df['Run_slide'].isin(include_list)]
        df = df[df['Plexity'] == 1000]

        df = df.reset_index(drop = True)

        return df

    def get_slide_metrics(self, df):
        '''Get analytics for per slide summary stats'''
        per_cell_tx = df['Per_cell_mean_true_genes'].sum() # + df['Per_cell_neg_probes'].sum() + df['Per_cell_false_codes'].sum()
        q90_per_cell_tx = df['Per_cell_Q90_true_genes'].sum()

        filtered = df[df['Match_eff_percent'] >= 3]
        filtered = df[df['Exp_error_percent'] <= 40]
        filtered = filtered[filtered['Match_error_percent'] <= 10]
        filtered = filtered[filtered['Per_cell_mean_true_genes']/filtered['Plexity'] >= 0.05]
        filtered = filtered[filtered['Per_cell_mean_neg_probes']/filtered['Plexity'] <= 0.1]
        filtered = filtered[filtered['Z1_step_percent_cycle_imgs_in_optimal_frame'] >= 78]
        filtered = filtered[filtered['Z1_step_standard_deviation'] <= 1.2]
        filtered = filtered[filtered['Unique_genes_per_cell_Q50'] >= 45]
        filtered = filtered[filtered['Calls_above_Neg_LOD'] >= 400]


        self.val_dict['Average_%_Pass_QC_per_Slide'] = (len(filtered) / len(df)) * 100
        self.val_dict['Number_of_Slides_Queried'] = len(df)
        self.val_dict['Average_Mean_Tx_per_Cell_per_Slide'] = per_cell_tx / len(df)
        self.val_dict['Average_Q90_Tx_per_Cell_per_Slide'] = q90_per_cell_tx / len(df)
        self.val_dict['Average_Genes_Detected_per_Slide'] = (df['Unique_genes_per_cell_Q100'].sum()) / len(df)

        return filtered

    def get_fov_metrics(self, df):
        '''Get analytics for per fov summary stats'''
        all_intra_tx = df['Intracellular_true_genes'].sum() # + df['Intracellular_false_codes'].sum() + df['Intracellular_neg_probes'].sum()
        unique_runs_list = list(set(df['Run_slide']))

        fov_per_slide_list = list()
        call_density_list = list()

        for run in unique_runs_list:
            single_run = df[df['Run_slide'] == run]
            fov_per_slide_list.append(len(single_run))

            total_row_transcripts_per_fov =  (single_run['Total_true_gene_transcripts'].sum() / len(single_run))
            if df['Instrument_type'][0] == 'DASH':
                call_density_list.append(total_row_transcripts_per_fov/(557133 * (6.4)))
            if df['Instrument_type'][0] == 'ALPHA':
                call_density_list.append(total_row_transcripts_per_fov/(570520 * (6.4)))
            if df['Instrument_type'][0] == 'BETA':
                call_density_list.append(total_row_transcripts_per_fov/(260835 * (6.4)))

        self.val_dict['Total_Cell_Number'] = df['Num_cells'].sum()
        self.val_dict['Total_Cell_Assigned_Transcripts'] = all_intra_tx
        self.val_dict['Average_Cells_per_FOV'] = (df['Num_cells'].sum() / len(df))
        self.val_dict['Average_#_FOVs_per_Slide'] = sum(fov_per_slide_list) / len(unique_runs_list)
        self.val_dict['Total_#_FOVs_collected'] = len(df)
        self.val_dict['Average_Cell_Number_per_Slide'] = df['Num_cells'].sum() / len(unique_runs_list)
        self.val_dict['Average_Tx_per_um^3_per_Slide'] = sum(call_density_list) / len(unique_runs_list)

        return

class SplitGraphCoefficients():
    '''Splits dataframe into different groupings and generates graphs and correlation tables for all data metrics'''
    def __init__(self, slide_db, fov_db):

        # if isinstance(fov_db, str):
        #     fov_db = pd.read_excel(fov_db)
        # fov = self.adjust_date(fov_db)
        # fov_filt = self.filter_table(fov)
        # fov_split = self.split_fov(fov)
        # fov_filt_split = self.split_fov(fov_filt)
        # self.regress(fov_split, filter = 'All_runs')
        # self.regress(fov_filt_split, filter = 'QC_filtered')

        if isinstance(slide_db, str):
            slide_db = pd.read_excel(slide_db)
        slide = self.adjust_date(slide_db)
        slide_filt = self.filter_table(slide)
        slide_split = self.split_slide(slide)
        slide_filt_split = self.split_slide(slide_filt)
        self.graph(slide_split, filter = 'All_runs')
        self.graph(slide_filt_split, filter = 'QC_filtered')

    def adjust_date(self, df):
        '''Trim entries to only include from 2022'''
        date_list = list()
        for date in df['Exp_start_date']:
            date_list.append(datetime.datetime.strptime(date, '%Y %m %d, %H:%M'))
        df['Exp_start_date'] = date_list

        df['Exp_start_date'] = pd.to_datetime(df['Exp_start_date'])
        df = df[(df['Exp_start_date'] > '2022-01-01 07:30:00')]
        df = df.reset_index(drop = True)
    
        return df

    def graph(self, df, filter):
        '''Generate html graphs of various grouping configurations'''
        color_cat_list = ['Tissue_type', 'Instrument_type']
        for plex in df:
            for category in df[plex]:
                for cat_type in df[plex][category]:

                    remove_list = ['Ch_HDR', 'Ch_norm', 'Ch_thresh', 'DeltaZ', 'Decoding_method', 'Owner_4', 'Owner_3',	'Owner_2',
                    'Bkg_sub_method', 'Zsteps', 'SW_version', 'Instrument_type', 'Instrument', 'Owner_1', 'Species', 'Storage', 
                    'Tissue_type', 'Run_directory_size_gb', 'Plexity', 'Voting_version', 'Exp_start_date', 'Origin_path', 'Run_slide_fov', 'Run_slide.1', 'Run_slide']

                    col_list = list(df[plex][category][cat_type].columns)
                    hold_list = list()
                    for item in col_list:
                        if '_sd' not in item:
                            hold_list.append(item)
                    col_list = hold_list
                    col_list = [x for x in col_list if x not in remove_list]

                    df[plex][category][cat_type] = df[plex][category][cat_type].replace(0, np.nan)

                    df[plex][category][cat_type]['Plexity'] = df[plex][category][cat_type]['Plexity'].astype(str)
                    for color_cat in color_cat_list:
                        for col in col_list:
                            if filter == 'All_runs':
                                Q1 = df[plex][category][cat_type][col].quantile(0.25)
                                Q3 = df[plex][category][cat_type][col].quantile(0.75)
                                IQR = Q3 - Q1
                                column_df = df[plex][category][cat_type][~(( df[plex][category][cat_type][col] < (Q1 - 1.5 * IQR)) |( df[plex][category][cat_type][col] > (Q3 + 1.5 * IQR)))]          
                            elif filter == 'QC_filtered':
                                column_df =  df[plex][category][cat_type]


                            fig = px.scatter(column_df, x = 'Exp_start_date', y = col, 
                            color = color_cat,
                            error_y = col + '_sd',
                            hover_name = 'Run_slide', hover_data = ['Tissue_type', 'Instrument_type', 'Instrument', 'Species', 'Storage', 'Owner_1'],
                            title = str(plex) + 'plex_' + col + '_over_time_n=' + str(len(column_df)))
                            fig.write_html('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\{}_graphs\\{}_{}_{}_{}_{}.html'.format(color_cat, plex, category, cat_type, col, filter))

        return

    def split_fov(self, df):
        '''Split FOV table into different groupings using a nested dictionary'''
        split_list = ['Tissue_type', 'Instrument_type', 'Species', 'Storage', 'Owner_1', 'Instrument']
        plexity_list = [100, 1000]

        all_categories_dict = {}
        for plex in plexity_list:
            plexed_df = df[df['Plexity'] == plex]
            plexed_df = plexed_df.reset_index(drop = True)
            if plex not in all_categories_dict:
                all_categories_dict[plex] = {}
            for column in split_list:
                if column not in all_categories_dict[plex]:
                    all_categories_dict[plex][column] = {}
                category_list = list(set(plexed_df[column]))
                try:
                    category_list.remove(0)
                except ValueError:
                    pass
                for category in category_list:
                    all_categories_dict[plex][column][category] = plexed_df[plexed_df[column] == category]
                    all_categories_dict[plex][column][category] = all_categories_dict[plex][column][category].reset_index(drop = True)

        all_categories_dict[100]['All_runs'] = {}
        all_categories_dict[1000]['All_runs'] = {}
        all_categories_dict[100]['All_runs']['All'] = df[df['Plexity'] == 100]
        all_categories_dict[1000]['All_runs']['All'] = df[df['Plexity'] == 1000]

        return all_categories_dict

    def split_slide(self, df):
        '''Split slide table into different groupings using a nested dictionary'''
        split_list = ['Tissue_type', 'Instrument_type']
        plexity_list = [100, 1000]

        all_categories_dict = {}
        for plex in plexity_list:
            plexed_df = df[df['Plexity'] == plex]
            plexed_df = plexed_df.reset_index(drop = True)
            if plex not in all_categories_dict:
                all_categories_dict[plex] = {}
            for column in split_list:
                if column not in all_categories_dict[plex]:
                    all_categories_dict[plex][column] = {}
                category_list = list(set(plexed_df[column]))
                try:
                    category_list.remove(0)
                    # category_list.remove(nan)
                except ValueError:
                    pass
                for category in category_list:
                    all_categories_dict[plex][column][category] = plexed_df[plexed_df[column] == category]
                    all_categories_dict[plex][column][category] = all_categories_dict[plex][column][category].reset_index(drop = True)

            all_categories_dict[plex] = {}
            all_categories_dict[plex]['All_runs'] = {}
            all_categories_dict[plex]['All_runs']['All'] = df[df['Plexity'] == plex]

        return all_categories_dict

    def filter_table(self, df):
        '''Filter entries based on compromised cut off values'''
        df = df[df['Match_eff_percent'] >= 3]
        df = df[df['Exp_error_percent'] <= 40]
        df = df[df['Match_error_percent'] <= 10]
        df = df[df['Per_cell_mean_true_genes']/df['Plexity'] >= 0.05]
        df = df[df['Per_cell_mean_neg_probes']/df['Plexity'] <= 0.1]
        df = df[df['Z1_step_percent_cycle_imgs_in_optimal_frame'] >= 78]
        df = df[df['Z1_step_standard_deviation'] <= 1.2]
        df = df[df['Calls_above_Neg_LOD'] >= 400]

        df = df.reset_index(drop = True)

        return df

    def regress(self, df, filter):
        '''Generate Pearson correlation coefficient for various groupings across time for each metric'''
        master_regress = pd.DataFrame()
        regress_df = pd.DataFrame()
        for plex in df:
            for category in df[plex]:
                for cat_type in df[plex][category]:

                    remove_list = ['Ch_HDR', 'Ch_norm', 'Ch_thresh', 'DeltaZ', 'Decoding_method',	'Owner_3',	'Owner_2',
                    'Bkg_sub_method', 'Zsteps', 'SW_version', 'Instrument_type', 'Instrument', 'Owner_1', 'Species', 'Storage', 
                    'Tissue_type', 'Run_directory_size_gb', 'Voting_version', 'Plexity', 'Exp_start_date', 'Origin_path', 'Run_slide', 'Run_slide.1', 'Unnamed: 0.1', 'Run_slide_fov']

                    df[plex][category][cat_type]['Exp_start_date'] = pd.to_datetime(df[plex][category][cat_type]['Exp_start_date'])
                    df[plex][category][cat_type]['Exp_start_date'] = df[plex][category][cat_type]['Exp_start_date'].map(datetime.datetime.toordinal)

                    df[plex][category][cat_type] = df[plex][category][cat_type].replace(0, np.nan)

                    zeroed_df = pd.DataFrame()
                    for col in df[plex][category][cat_type].columns:
                        zeroed_df.at[0, col] = cat_type
                        dropped_df = df[plex][category][cat_type].dropna(subset = [col])
                        entry_num = len(list(set(df[plex][category][cat_type]['Run_slide'])))
                        if col not in remove_list:
                            if len(list(set(dropped_df['Exp_start_date']))) > 1:
                                slope, intercept, r_value, p_value, std_err = stats.linregress(dropped_df['Exp_start_date'], dropped_df[col])
                                regress_df.at[col, '{}plex_{}_n={}'.format(plex, cat_type, entry_num)] = r_value

                    master_regress = pd.concat([master_regress, zeroed_df])
                    master_regress = pd.concat([master_regress, df[plex][category][cat_type]])

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Correlation_tables\\Correlation_table_{}_v5.xlsx'.format(filter), engine='xlsxwriter')
        regress_df.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Correlation_tables\\Tabulated_groupings_for_correlation_analysis_{}_v5.xlsx'.format(filter), engine='xlsxwriter')
        master_regress.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

        return

class RegressAll():
    def __init__(self, fov_db):
    
        if isinstance(fov_db, str):
            fov_db = pd.read_excel(fov_db)

        df = self.adjust_date(fov_db)

        df = self.filter_entries(df)
        self.regress_all_variables(df)

    def adjust_date(self, df):
        '''Trim entries to only include from 2022'''
        date_list = list()
        for date in df['Exp_start_date']:
            date_list.append(datetime.datetime.strptime(date, '%Y %m %d, %H:%M'))
        df['Exp_start_date'] = date_list

        df['Exp_start_date'] = pd.to_datetime(df['Exp_start_date'])
        df = df[(df['Exp_start_date'] > '2022-01-01 07:30:00')]
        df = df.reset_index(drop = True)
    
        return df

    def filter_entries(self, df):
        '''Filter entries based on compromised cut off values'''
        df = df[df['Match_eff_percent'] >= 3]
        df = df[df['Exp_error_percent'] <= 40]
        df = df[df['Match_error_percent'] <= 10]
        df = df[df['Per_cell_mean_true_genes']/df['Plexity'] >= 0.05]
        df = df[df['Per_cell_mean_neg_probes']/df['Plexity'] <= 0.1]
        df = df[df['Z1_step_percent_cycle_imgs_in_optimal_frame'] >= 78]
        df = df[df['Z1_step_standard_deviation'] <= 1.2]
        df = df[df['Calls_above_Neg_LOD'] >= 400]
        df = df[df['Plexity'] == 1000]

        df = df.reset_index(drop = True)

        return df

    def regress_all_variables(self, df):

        regress_df = pd.DataFrame()

        remove_list = ['Ch_HDR', 'Ch_norm', 'Ch_thresh', 'DeltaZ', 'Decoding_method',	'Owner_3',	'Owner_2',
        'Bkg_sub_method', 'Zsteps', 'SW_version', 'Instrument_type', 'Instrument', 'Owner_1', 'Species', 'Storage', 
        'Tissue_type', 'Run_directory_size_gb', 'Voting_version', 'Plexity', 'Exp_start_date', 'Origin_path', 'Run_slide', 'Run_slide.1', 'Unnamed: 0.1', 'Run_slide_fov']

        col_list = [item for item in list(df.columns) if item not in remove_list]

        col_list_combo = itertools.combinations(col_list, 2)

        df = df.replace(0, np.nan)

        for combo in col_list_combo:
            slope, intercept, r_value, p_value, std_err = stats.linregress(df[combo[0]], df[combo[1]])
            regress_df.at[combo[0], combo[1]] = r_value

        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Correlation_tables\\All_variables_against_each_other.xlsx'.format(filter), engine='xlsxwriter')
        regress_df.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()  

if __name__ == "__main__":

    # Output and input paths for checkpoint processing
    slide_raw_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Raw\Transcript_slide_v5.csv'
    fov_raw_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Raw\Transcript_fov_v5.csv'
    qb_ff_meta_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Metadata\QuickBase_FF.xlsx'
    qb_ffpe_meta_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Metadata\QuickBase_FFPE.xlsx'
    master_meta_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Metadata\CompiledSecondaryData_cleaned.xlsx'
    fov_cleaned_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Cleaned\Transcript_fov_db_v5.xlsx'
    slide_cleaned_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Cleaned\Transcript_slide_db_v5.xlsx'
    master_merged_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Cleaned\Merged with meta\CompiledSecondaryData_fov_full_merge_v5.xlsx'
    meta_fov_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Cleaned\Merged with meta\Complete_fov_meta_v5.xlsx'
    meta_slide_path = r'C:\Users\crobbins\Desktop\Cosmx_db\Compiled_db\Cleaned\Merged with meta\Complete_slide_meta_v5.xlsx'

    # CleanTable(input_db = slide_raw_path, format = 'Run_slide')
    # CleanTable(input_db = fov_raw_path, format = 'Run_slide_fov')
    # MergeMeta(slide_cleaned_path, fov_cleaned_path, qb_ff_meta_path, qb_ffpe_meta_path, master_meta_path)
    # FindDifference(master_merged_path)
    SplitGraphCoefficients(meta_slide_path, meta_fov_path)
    # SummaryStats(meta_slide_path, meta_fov_path)
    # RegressAll(meta_fov_path)




class PullFromFOV():
    '''Generates a new summary slide table from FOV entries'''
    def __init__(self, fov):
        
        if isinstance(fov, str):
            df = pd.read_excel(fov, index_col = 'Run_slide_fov')
        else:
            df = fov

        run_slide_list = list()
        for row in range(len(df)):
            run_slide_list.append(df.index[row][:-5])
        df['Run_slide'] = run_slide_list


        remove_list = ['Ch_HDR', 'Ch_norm', 'Ch_thresh', 'DeltaZ', 'Decoding_method',	'Owner_3',	'Owner_2',
        'Bkg_sub_method', 'Zsteps', 'Voting_version', 'SW_version', 'Instrument_type', 'Instrument', 'Owner_1', 'Species', 'Storage', 
        'Tissue_type', 'Run_directory_size_gb', 'Plexity', 'Exp_start_date', 'Origin_path', 'Run_slide']

        # col_reorder_dict = {}
        # for item in remove_list:
        #     col_reorder_dict[item] = df[item]

        # dropped = df.drop(remove_list, axis = 1)
        # dropped = dropped.reindex(sorted(dropped.columns), axis=1)

        master_slide = pd.DataFrame()
        for run_slide in run_slide_list:
            single_slide_df = df[df['Run_slide'] == run_slide]
            for col in single_slide_df.columns:
                if col not in remove_list:
                    master_slide.at[run_slide, col] = single_slide_df[col].mean()
                    master_slide.at[run_slide, col + '_sd'] = single_slide_df[col].std()
                else:
                    master_slide.at[run_slide, col] = list(set(single_slide_df[col]))[0]

        # master_slide.index.name = 'Run_slide'

        # for item in remove_list:
        #     master_slide.insert(0, item, col_reorder_dict[item])

        self.df = master_slide  
        self.df.index.name = 'Run_slide'
 
        writer = pd.ExcelWriter('C:\\Users\\crobbins\\Desktop\\Cosmx_db\\Compiled_db\\Cleaned\\Transcript_slide_added_v5.xlsx', engine='xlsxwriter')
        self.df.to_excel(writer,  sheet_name = 'Sheet 1')
        writer.save()

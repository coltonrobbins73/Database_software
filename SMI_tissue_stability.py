print('Compiling packages and building GUI...')
from scipy import stats
import PySimpleGUI as sg
import pandas as pd
import re
import numpy as np
import skimage.draw as draw
import skimage.io as skio
import cv2
import pathlib
import tifffile as tiff
import gc
import pandas as pd
import numpy as np
import re
import os
from collections import Counter
import math
import seaborn as sns
import xlsxwriter
from matplotlib import pyplot as plt
from natsort import natsorted
import PySimpleGUI as sg
import time
import skimage
import io
import matplotlib.backends.backend_pdf
import string
import time
import tifffile as tiff
import itertools
import os
import imutils

class bcolors:
    os.system("")
    # set color codes for text
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class MainUI:
    """Main GUI window for SMI analysis package"""
    def __init__(self):
        self.input_directory, self.wobble_depth, self.fov_list, self.rep_list, self.color_list = self.__get_params()        

    def __get_params(theme=None):
        '''UI for parameter input'''

        tab5_layout = [
        [sg.T('***Generates per FOV stability scores for all cycles grouped by reporter pool***')],
        [sg.T('Requirements: ProjDir')],
        [sg.InputText(r'C:\path_to_run\Run0001', key='Run_directory'), sg.FolderBrowse('Select folder')],
        [sg.Text('_' * 80)],
        [sg.Text('-----List of outputs-----')],
        [sg.Column([[sg.T('>Absolute blurriness score per FOV per cycle')],
        [sg.T('>Graphical summary for each FOV')],
        [sg.T('>Graphical summary for each slide')],
        [sg.T(' ' * 20)], [sg.T(' ' * 20)], [sg.T(' ' * 20)], [sg.T(' ' * 20)]], element_justification='l', scrollable = True)],
        [sg.T('Select your analysis depth')],
        [sg.Radio('Quick (first and last cycle)', 'depth', default=True, key = 'quick_wobble')],
        [sg.Radio('In-depth (all cycles)', 'depth', default=False, key = 'long_wobble')],
        [sg.T('_' * 80)],
        [sg.T('Select your desired channels')],
        [sg.Checkbox('BB', default=False, key = 'BB')],
        [sg.Checkbox('GG', default=False, key = 'GG')],
        [sg.Checkbox('YY', default=True, key = 'YY')],
        [sg.Checkbox('RR', default=False, key = 'RR')],
        [sg.T('_' * 80)],
        [sg.T('Select your desired reporters and FOVs')],
        [sg.T('**Note** scoring is consistent between reporters unless there is substantial crowding\nUse multi-reporter analysis to identify crowded reporter images by looking for a significant increase in score')],
        [sg.Text('Reporter_pool'), sg.In(default_text=1 ,size = (25,1), key = 'rep_entry')],
        [sg.Text('FOVs'), sg.In(default_text = 'All', size = (26,1), key = 'fov_entry')],
        [sg.T('' * 80)],
        [sg.T('' * 80)],
        ]

        layout = [
        [sg.TabGroup([[
                    sg.Tab('Tissue stability analysis', tab5_layout),
                    ]])],
        [sg.Button('Run'), sg.Button('Cancel')]]

        window = sg.Window('SMI analysis package', layout, finalize=True, right_click_menu=sg.MENU_RIGHT_CLICK_EDITME_VER_EXIT, keep_on_top=True)

        while True:
            event, values = window.read()
            # sg.Print(event, values)
            
            # Save settings buttons
            if event in ('Run', None):
                break
            elif event in ('Cancel', None):
                os._exit(0)
                break

        window.CloseNonBlocking() 

        wobble_fov_target_ls = list()
        hold_list = list()
        if values['fov_entry'] == 'All':
            wobble_fov_target_ls = 'All'
        else:
            for num in values['fov_entry'].split(','):
                hold_list.append(re.findall(r'\d+', num))
            for item in hold_list:
                if len(item) == 1:
                    if int(item[0]) < 10:
                        wobble_fov_target_ls.append('F0' + '0' + str(item[0]))
                    elif int(item[0]) < 100:
                        wobble_fov_target_ls.append('F0' + str(item[0]))
                    else:
                        wobble_fov_target_ls.append('F' + str(item[0]))
                else:
                    for dash_num in range(int(item[0]), int(item[1]) + 1):
                        if dash_num < 10:
                            wobble_fov_target_ls.append('F0' + '0' + str(dash_num))
                        elif dash_num < 100:
                            wobble_fov_target_ls.append('F0' + str(dash_num))
                        else:
                            wobble_fov_target_ls.append('F' + str(dash_num))

        wobble_rep_target_ls = list()
        hold_list = list()
        if values['rep_entry'] != '':
            for num in values['rep_entry'].split(','):
                hold_list.append(re.findall(r'\d+', num))
            for item in hold_list:
                if len(item) == 1:
                    if ((int(item[0]) * 2)-1) < 10:
                        wobble_rep_target_ls.append('N0' + str(((int(item[0]) * 2)-1)))
                    else:
                        wobble_rep_target_ls.append('N' + str(((int(item[0]) * 2)-1)))
                else:
                    for dash_num in range(int(item[0]), int(item[1]) + 1):
                        if (dash_num * 2)-1 < 10:
                            wobble_rep_target_ls.append('N0' + str((dash_num * 2)-1))
                        else:
                            wobble_rep_target_ls.append('N' + str((dash_num * 2)-1))

        if values['quick_wobble'] == True:
            wobble_depth = 'quick'
        else:
            wobble_depth = 'long'
        wobble_color_list = list()
        colors = ['BB', 'GG', 'YY', 'RR']
        for color in colors:
            if values[color] == True:
                wobble_color_list.append(color)

        return values['Run_directory'], wobble_depth, wobble_fov_target_ls, wobble_rep_target_ls, wobble_color_list

class FindFiles:
    '''Generates a list of file paths by file extension'''
    def __init__(self, input_directory):
        print('\n\n\nCompiling all file paths for run data...')
        self.input_directory = input_directory
        self.tif = self.__find_files()
    def __find_files(self):
        # Init all possible extension lists
        tif_paths = {}
        # check_re_fun = re.compile(r'CellOverlay|complete_code_cell_target_call_coord|ProjDir|SpotFiles|CellStatsDir|SpatialBC|Analysis_Summary')
        check_re_fun = re.compile(r'ProjDir')
        # Use rglob to crawl through directory tree and extract specific files
        for root, dirs, files in os.walk(self.input_directory, topdown=False):
            for name in files:
                checked = check_re_fun.search(root + name)
                if checked is not None:
                    tif_paths[name] = root + '\\' + name
        
        print(f"\n\n\nTotal number of found projected images for this run ---{bcolors.FAIL}%s{bcolors.ENDC}--- " % (len(tif_paths)))

        return tif_paths

class LabelFiles:
    def __init__(self, file_paths, re_check, 
    slide_list = None, cycle_list = None, fov_list = None, rep_list = None, exclusion = None):
        self.labeled_paths = {}
        file_paths = file_paths
        # Long list of regex functions
        check_dict = {'cell_overlay' : r'CellOverlay', 'voted_genes' : r'complete_code_cell_target_call_coord',
        'proj_dir' : r'ProjDir', 'spot_files' : r'SpotFiles', 'cell_stats' : r'(?=.*CellStatsDir)(?=.*Summary)',
        'spatialBC' : r'SpatialBC', 'matching' : r'Analysis_Summary'}
        check_re_fun = re.compile(check_dict[re_check])

        if re_check in ['voted_genes', 'matching']:
            fov_re_fun = re.compile(r'FOV\d{3}')
        if re_check in ['cell_overlay', 'spot_files', 'cell_stats', 'proj_dir']:
            fov_re_fun = re.compile(r'F\d{3}')
        run_re_fun = re.compile(r'Run\d{4}')
        alpha_re_fun = re.compile(r'RunA\d{4}')
        slide_re_fun = re.compile(r'_S\d{1}')
        slide2_re_fun = re.compile(r'\\S\d{1}')
        rep_re_fun = re.compile(r'N\d{2}')
        cycle_re_fun = re.compile(r'C\d{3}')
        zslice_re_fun = re.compile(r'[Z][mp]\d{2}')        
        
        for file in file_paths:
            str_path = str(file_paths[file])
            # Check that target keyword is in the path
            check = check_re_fun.search(str_path)
            if check is None:
                continue
            if file == 'Thumbs.db':
                continue
            run = run_re_fun.search(str_path)
            # Extra check for alpha run formats
            if run is None:
                run = alpha_re_fun.search(str_path)
                run = run.group()
                run = run[4:]
                run = 'Run' + run
            else:
                run = run.group()
            str_path = file
            slide = slide_re_fun.search(str_path)
            # Extra check to skip slide regex in experiment description
            if slide is None:
                slide = slide2_re_fun.search(str_path)
                slide = slide.group()
                slide = slide[1:]
            else:    
                slide = slide.group()
                slide = slide[1:]
            if slide_list:
                if slide not in slide_list:
                    continue
            if re_check != 'spatialBC':
                fov = fov_re_fun.search(str_path)
                fov = fov.group()
            # Select the correct fov format based on target search
            if re_check in ['voted_genes', 'matching']:
                fov = fov[3:]
                fov = 'F' + fov
            if fov_list:
                if fov not in fov_list:
                    continue
            if re_check in ['spot_files', 'proj_dir']:
                cycle = cycle_re_fun.search(str_path)
                cycle = cycle.group()
                if cycle_list:
                    if cycle not in cycle_list:
                        continue
            if re_check in ['spot_files', 'proj_dir']:
                rep = rep_re_fun.search(str_path)
                rep = rep.group()
                if rep_list:
                    if rep not in rep_list:
                        continue
                # Extract number value from reporter string. If num is even, skip
                if (int(re.findall(r'\d+', rep)[0]) % 2) == 0:
                    continue 
            # Exclude failed fovs if labeling summary statistic files
            if exclusion is not None:
                if slide in list(exclusion.keys()) and fov in exclusion[slide]:
                    continue
            if re_check == 'spot_files':
                zslice = zslice_re_fun.search(str_path)
                zslice = zslice.group()
            # Create nested dictionaries for each identifier
            if run not in self.labeled_paths:
                self.labeled_paths[run] = {}                    
            if slide not in self.labeled_paths[run]:
                self.labeled_paths[run][slide] = {}
            if re_check == 'spatialBC':
                self.labeled_paths[run][slide] = file
            if re_check in ['cell_overlay', 'voted_genes', 'cell_stats', 'matching']:
                if fov not in self.labeled_paths[run][slide]:
                    self.labeled_paths[run][slide][fov] = file
            if re_check == 'proj_dir':
                if cycle not in self.labeled_paths[run][slide]:
                    self.labeled_paths[run][slide][cycle] = {}
                if fov not in self.labeled_paths[run][slide][cycle]:
                    self.labeled_paths[run][slide][cycle][fov] = {}
                if rep not in self.labeled_paths[run][slide][cycle][fov]:
                    self.labeled_paths[run][slide][cycle][fov][rep] = file_paths[file]
            if re_check == 'spot_files':
                if cycle not in self.labeled_paths[run][slide]:
                    self.labeled_paths[run][slide][cycle] = {}
                if fov not in self.labeled_paths[run][slide][cycle]:
                    self.labeled_paths[run][slide][cycle][fov] = {}
                if rep not in self.labeled_paths[run][slide][cycle][fov]:
                    self.labeled_paths[run][slide][cycle][fov][rep] = {}
                if zslice not in self.labeled_paths[run][slide][cycle][fov][rep]:
                    self.labeled_paths[run][slide][cycle][fov][rep][zslice] = file    

class Wobble:
    '''Function to measure changes in blurriness from cycle images'''
    def __init__(self, output_directory, tif, depth, fov_list, rep_list, color_list):
        self.start_time = time.time()
        print("\n\n\nCommencing wobble analysis...")
        self.output_directory = output_directory
        self.depth = depth
        self.fov_list = fov_list
        self.rep_list = rep_list
        self.color_list = color_list    
        self.tif_paths = LabelFiles(file_paths = tif, re_check = 'proj_dir')

        self.wobble = self.__calculate_wobble()
        print("\n\nWobble scores calculated --- %s seconds ---" % (time.time() - self.start_time))
        self.start_time = time.time()

    def __calculate_wobble(self):

        if not os.path.exists(self.output_directory + '\\Wobble_tables'):
            os.mkdir(self.output_directory + '\\Wobble_tables_test')
        if not os.path.exists(self.output_directory + '\\Wobble_graphs'):
            os.mkdir(self.output_directory + '\\Wobble_graphs_test')

        def detect_blur_fft(image, size=60):
            # Grab the dimension to compute center of image
            (h, w) = image.shape
            (cX, cY) = (int(w / 2.0), int(h / 2.0))
            # Compute FFT and then shift the low frequency (DC component) values to the center of the image
            fft = np.fft.fft2(image)
            fftShift = np.fft.fftshift(fft)
            # Create a blacking box to remove all low frequency signal from the image
            fftShift[cY - size:cY + size, cX - size:cX + size] = 0
            # Inverse shift back to normal viewing
            fftShift = np.fft.ifftshift(fftShift)
            # Inverse FFT to return to spatial domain
            recon = np.fft.ifft2(fftShift)
            # Measure the absoolute amplitude of the signal frequency
            magnitude = 20 * np.log(np.abs(recon))
            # Take the mean of that population
            mean = np.mean(magnitude)
            # Typical values range from 80-120 with good quality acheived at around 107
            return mean

        # Reorder dictionary to isolate all reporters in each cycle
        single_rep_dict = {}
        for run in self.tif_paths.labeled_paths:
            for slide in self.tif_paths.labeled_paths[run]:
                for cycle in self.tif_paths.labeled_paths[run][slide]:
                    for fov in self.tif_paths.labeled_paths[run][slide][cycle]:
                        for rep in self.tif_paths.labeled_paths[run][slide][cycle][fov]:
                            if run not in single_rep_dict:
                                single_rep_dict[run] = {}
                            if slide not in single_rep_dict[run]:
                                single_rep_dict[run][slide] = {}
                            if rep not in single_rep_dict[run][slide]:
                                single_rep_dict[run][slide][rep] = {}
                            if fov not in single_rep_dict[run][slide][rep]:
                                single_rep_dict[run][slide][rep][fov] = {}
                            if cycle not in single_rep_dict[run][slide][rep][fov]:
                                single_rep_dict[run][slide][rep][fov][cycle] = self.tif_paths.labeled_paths[run][slide][cycle][fov][rep]

        warnings = {}
        wobble = {}
        for run in single_rep_dict:
            for slide in single_rep_dict[run]:
                warnings[slide] = {}
                wobble[slide] = {}
                for rep in single_rep_dict[run][slide]:
                    if rep not in self.rep_list:
                        continue
                    wobble[slide][rep] = {}
                    warnings[slide][rep] = {}
                    for fov in single_rep_dict[run][slide][rep]:
                        if self.fov_list == 'All':
                            self.fov_list = list(single_rep_dict[run][slide][rep].keys())
                        if fov not in self.fov_list:
                            continue
                        if self.depth == 'quick':
                            cycle_list = list(single_rep_dict[run][slide][rep][fov].keys())
                            first_last = list()
                            first_last.append(cycle_list[0])
                            first_last.append(cycle_list[len(cycle_list)-1])
                            target_cycles = first_last
                        else:
                            target_cycles = list(single_rep_dict[run][slide][rep][fov].keys())
                        for cycle in target_cycles:
                            image = skio.imread(single_rep_dict[run][slide][rep][fov][cycle])
                            BB, GG, YY, RR =cv2.split(image)
                            colors = {'BB' : BB, 'GG' : GG, 'YY' : YY, 'RR' : RR}
                            for color in colors:
                                if color not in self.color_list:
                                    continue
                                if color not in wobble[slide][rep]:
                                    wobble[slide][rep][color] = pd.DataFrame()
                                if color not in warnings[slide][rep]:
                                    warnings[slide][rep][color] = pd.DataFrame()
                                # FFT
                                # apply our blur detector using the FFT
                                score = detect_blur_fft(colors[color], size=60)
                                score = round(score, 4)
                                current_process_lable = slide +'_'+ fov + '_' + rep + '_' + cycle + '_' + color
                                if score < 69:
                                    print(f"{bcolors.WARNING}**Warning** Possibe water column failure detected for{bcolors.FAIL} %s{bcolors.ENDC} " % (current_process_lable))
                                    warnings[slide][rep][color].at[cycle, fov] = 'WC fail'  
                                elif score < 92: 
                                    print(f"{bcolors.WARNING}**Warning** Severe tissue instability detected for{bcolors.FAIL} %s{bcolors.ENDC} " % (current_process_lable))
                                    warnings[slide][rep][color].at[cycle, fov] = 'Instable'
                                elif score < 105:
                                    print(f"{bcolors.WARNING}**Warning** Moderate tissue instability detected for{bcolors.FAIL} %s{bcolors.ENDC} " % (current_process_lable))
                                    warnings[slide][rep][color].at[cycle, fov] = 'Moderate'
                                else:
                                    warnings[slide][rep][color].at[cycle, fov] = 'Stable'

                                wobble[slide][rep][color].at[int(re.findall(r'\d+', cycle)[0]), fov] = score
                                print('Absolute blurriness index for: {}_{}_{}_{}_{} ---{:.4f}---\n'.format(slide, fov, rep, cycle, color, score))

                writer = pd.ExcelWriter(self.output_directory + '\\Wobble_tables_test\\' + '{}_blur_score_by_rep.xlsx'.format(slide),engine='xlsxwriter')   
                startrow = 0
                for rep in wobble[slide]:
                    for color in wobble[slide][rep]:
                        wobble[slide][rep][color].to_excel(writer,sheet_name = rep, startrow = startrow)
                        color_df = pd.DataFrame({color})
                        color_df.to_excel(writer,sheet_name = rep, startrow = startrow - 1, startcol= -1)
                        startrow = startrow + wobble[slide][rep][color].shape[0] + 2
                    for sheet in writer.sheets:
                        writer.sheets[sheet].conditional_format('B1:ZZ100', {'type': '3_color_scale',
                                        'min_value': 80,
                                        'max_value': 120})
                writer.save()

                writer = pd.ExcelWriter(self.output_directory + '\\Wobble_tables_test\\' + '{}_Instability_warnings.xlsx'.format(slide),engine='xlsxwriter')   
                startrow = 0
                for rep in warnings[slide]:
                    for color in warnings[slide][rep]:
                        warnings[slide][rep][color].to_excel(writer,sheet_name = rep, startrow = startrow)
                        color_df = pd.DataFrame({color})
                        color_df.to_excel(writer,sheet_name = rep, startrow = startrow - 1, startcol= -1)
                        startrow = startrow + warnings[slide][rep][color].shape[0] + 2
                writer.save()

                slope_quart = {}
                slope_quart[slide] = {}
                for rep in wobble[slide]:
                    slope_quart[slide][rep] = {}
                    for color in wobble[slide][rep]:
                        slope_quart[slide][rep][color] = pd.DataFrame()
                        for col in wobble[slide][rep][color].columns:
                            slope, intercept, r_value, p_value, std_err = stats.linregress(wobble[slide][rep][color][col].index, wobble[slide][rep][color][col])
                            slope_quart[slide][rep][color].at[col, 'Slope'] = slope
                        slope_quart[slide][rep][color] = slope_quart[slide][rep][color].sort_values(by = 'Slope')

                        chunked_fovs = np.array_split(np.array(list(slope_quart[slide][rep][color].index)),4)
                        slope_quart[slide][rep][color] = {}
                        for i, chunk in zip(range(1, 5), chunked_fovs):
                            slope_quart[slide][rep][color][i] = wobble[slide][rep][color].loc[:, chunk]
                        
                        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, sharex = 'col', sharey = 'row')
                        fig.suptitle('{}_{}_{}_delta_stability_score'.format(slide, rep, color))
                        ax1.plot(slope_quart[slide][rep][color][1])
                        ax2.plot(slope_quart[slide][rep][color][2])
                        ax3.plot(slope_quart[slide][rep][color][3])
                        ax4.plot(slope_quart[slide][rep][color][4])
                        ax1.set_title('1st quartile')
                        ax2.set_title('2nd quartile')
                        ax3.set_title('3rd quartile')
                        ax4.set_title('4th quartile')
                        ax1.legend(list(slope_quart[slide][rep][color][1].columns))
                        ax2.legend(list(slope_quart[slide][rep][color][2].columns))
                        ax3.legend(list(slope_quart[slide][rep][color][3].columns))
                        ax4.legend(list(slope_quart[slide][rep][color][4].columns))

                        plt.savefig(self.output_directory + '\\Wobble_graphs_test\\' '{}_rep{}_slope_graphs.pdf'.format(slide, rep, color))
                        plt.close()

                for slide in wobble:
                    slope_quart = {}
                    slope_quart[slide] = {}
                    for rep in wobble[slide]:
                        slope_quart[slide][rep] = {}
                        for color in wobble[slide][rep]:
                            slope_quart[slide][rep][color] = pd.DataFrame()
                            for col in wobble[slide][rep][color].columns:
                                slope, intercept, r_value, p_value, std_err = stats.linregress(wobble[slide][rep][color][col].index, wobble[slide][rep][color][col])
                                slope_quart[slide][rep][color].at[col, 'Slope'] = slope
                            slope_quart[slide][rep][color] = slope_quart[slide][rep][color].sort_values(by = 'Slope')

                            chunked_fovs = np.array_split(np.array(list(slope_quart[slide][rep][color].index)),4)
                            slope_quart[slide][rep][color] = {}
                            for i, chunk in zip(range(1, 5), chunked_fovs):
                                slope_quart[slide][rep][color][i] = wobble[slide][rep][color].loc[:, chunk]
                            
                            fig, (ax1, ax2 , ax3, ax4) = plt.subplots(1, 4, figsize = (40,7), sharey = True)
                            fig.suptitle('{}_{}_{}_delta_stability_score'.format(slide, rep, color))
                            ax1.plot(slope_quart[slide][rep][color][1])
                            ax2.plot(slope_quart[slide][rep][color][2])
                            ax3.plot(slope_quart[slide][rep][color][3])
                            ax4.plot(slope_quart[slide][rep][color][4])
                            ax1.set_title('1st quartile')
                            ax2.set_title('2nd quartile')
                            ax3.set_title('3rd quartile')
                            ax4.set_title('4th quartile')
                            ax1.legend(list(slope_quart[slide][rep][color][1].columns))
                            ax2.legend(list(slope_quart[slide][rep][color][2].columns))
                            ax3.legend(list(slope_quart[slide][rep][color][3].columns))
                            ax4.legend(list(slope_quart[slide][rep][color][4].columns))

                            plt.savefig(self.output_directory + '\\Wobble_graphs_test\\' '{}_rep{}_{}_slope_graphs.pdf'.format(slide, rep, color))
                            plt.close()

if __name__ == "__main__":

    input = MainUI()
    all_files = FindFiles(input_directory = input.input_directory)

    Wobble(output_directory = input.input_directory, tif = all_files.tif, depth = input.wobble_depth, fov_list = input.fov_list, rep_list = input.rep_list, color_list = input.color_list)



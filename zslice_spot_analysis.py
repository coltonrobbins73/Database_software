print('Compiling packages...')
from sys import maxsize
import pandas as pd
import re
import numpy as np
import os
from scipy import stats
import win32api
import win32wnet, win32netcon
from pandas.errors import EmptyDataError 
import statistics
import csv
import tifffile as tiff
import skimage
import skimage.draw as draw
import skimage.io as skio
import pathlib
import cv2
import gc
import matplotlib.pyplot as plt 
import matplotlib
matplotlib.use('Agg')
from matplotlib.pyplot import figure
from cv2_rolling_ball import subtract_background_rolling_ball
from io import StringIO
import matplotlib.ticker as mtick

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
        check_re_fun = re.compile(r'CellOverlay|SpotFiles|ProjDir|Images|3DReg_Results.csv|Jitter_Results.csv|CurrentSpotLists.txt')
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

        self.cell, self.proj, self.spot, self.z, self.global_shift, self.channel_shift, self.zrange = self.label_files(file_paths)

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
        exclusion_re_fun = re.compile(r'ProjDir_Test|LoG|LoG.txt|.log|CurrentSpotLists|Metrics3D|.pdf|.png|after_Z|.pickle|.pb|cellMatrix|Thumbs|.db|edited|Copy|TAPDataComp|Manual Segmentation|Summary_Compiled')

        run_re_fun = re.compile(r'Run\d{4}A|Run\d{4}|RunA\d{4}|RunB\d{4}|A\d{4}|B\d{4}|R\d{4}')
        slide_re_fun = re.compile(r'_S\d{1}|S\d{1}')
        f_re_fun = re.compile(r'F\d{3}')
        rep_re_fun = re.compile(r'N\d{2}')
        cycle_re_fun = re.compile(r'C\d{3}')
        zslice_re_fun = re.compile(r'[Z][mp]\d{2}') 
        img_zslice_re_fun = re.compile(r'Z\d{3}') 

        cell = {}
        proj = {}
        spot = {}
        z = {}
        global_shift = {}
        channel_shift = {}
        zrange = {}
        
        for file in file_paths:
            # Make sure path is in string format for regex
            full_path = str(file)
            file_name = str(file_paths[file])
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
            # if slide == 'S1':
            #     continue
            fov = f_re_fun.search(file_name).group()
            fov_num = int(re.findall(r'\d+', fov)[0])
            if fov_num not in [1,2,3]:
                continue 
            target_file_list = ['SpotFiles', 'ProjDir', 'Images', '3DReg_Results', 'Jitter_Results']
            if any(substring in full_path for substring in target_file_list):
                cycle = cycle_re_fun.search(file_name)
                cycle = cycle.group()
                rep = rep_re_fun.search(file_name)
                rep = rep.group()
                rep_num = int(re.findall(r'\d+', rep)[0])
                cycle_num = int(re.findall(r'\d+', cycle)[0])
                # Extract number value from reporter string. If num is even, skip
                if (rep_num % 2) == 0:
                    continue 
                # if rep_num < 19:
                #     continue 
                # if cycle_num < 5:
                #     continue 
            if 'CellOverlay' in full_path:
                if run not in cell:
                    cell[run] = {}
                if slide not in cell[run]:
                    cell[run][slide] = {}
                if fov not in cell[run][slide]:
                    cell[run][slide][fov] = file
            if 'CurrentSpotLists' in full_path:
                if run not in zrange:
                    zrange[run] = {}
                if slide not in zrange[run]:
                    zrange[run][slide] = {}
                if fov not in zrange[run][slide]:
                    zrange[run][slide][fov] = file
            if 'ProjDir' in full_path:
                if run not in proj:
                    proj[run] = {}
                if slide not in proj[run]:
                    proj[run][slide] = {}
                if cycle not in proj[run][slide]:
                    proj[run][slide][cycle] = {}
                if fov not in proj[run][slide][cycle]:
                    proj[run][slide][cycle][fov] = {}
                if rep not in proj[run][slide][cycle][fov]:
                    proj[run][slide][cycle][fov][rep] = file
            if 'SpotFiles' in full_path:
                zslice = zslice_re_fun.search(file_name)
                zslice = zslice.group()
                if run not in spot:
                    spot[run] = {}
                if slide not in spot[run]:
                    spot[run][slide] = {}
                if cycle not in spot[run][slide]:
                    spot[run][slide][cycle] = {}
                if fov not in spot[run][slide][cycle]:
                    spot[run][slide][cycle][fov] = {}
                if rep not in spot[run][slide][cycle][fov]:
                    spot[run][slide][cycle][fov][rep] = {}
                if zslice not in spot[run][slide][cycle][fov][rep]:
                    spot[run][slide][cycle][fov][rep][zslice] = file  
            if 'Images' in full_path:
                if cycle_num in [0, 901, 902]:
                    continue
                img_zslice = img_zslice_re_fun.search(file_name)
                img_zslice = img_zslice.group()
                z_num = int(re.findall(r'\d+', img_zslice)[0])
                if run not in z:
                    z[run] = {}
                if slide not in z[run]:
                    z[run][slide] = {}
                if cycle not in z[run][slide]:
                    z[run][slide][cycle] = {}
                if fov not in z[run][slide][cycle]:
                    z[run][slide][cycle][fov] = {}
                if rep not in z[run][slide][cycle][fov]:
                    z[run][slide][cycle][fov][rep] = {}
                if img_zslice not in z[run][slide][cycle][fov][rep]:
                    z[run][slide][cycle][fov][rep][img_zslice] = file
            if '3DReg_Results' in file_name:
                if run not in global_shift:
                    global_shift[run] = {}
                if slide not in global_shift[run]:
                    global_shift[run][slide] = {}
                if cycle not in global_shift[run][slide]:
                    global_shift[run][slide][cycle] = {}
                if fov not in global_shift[run][slide][cycle]:
                    global_shift[run][slide][cycle][fov] = {}
                if rep not in global_shift[run][slide][cycle][fov]:
                    global_shift[run][slide][cycle][fov][rep] = file
            if 'Jitter_Results' in file_name:
                if run not in channel_shift:
                    channel_shift[run] = {}
                if slide not in channel_shift[run]:
                    channel_shift[run][slide] = {}
                if cycle not in channel_shift[run][slide]:
                    channel_shift[run][slide][cycle] = {}
                if fov not in channel_shift[run][slide][cycle]:
                    channel_shift[run][slide][cycle][fov] = {}
                if rep not in channel_shift[run][slide][cycle][fov]:
                    channel_shift[run][slide][cycle][fov][rep] = file

        return cell, proj, spot, z, global_shift, channel_shift, zrange

class ScrapeTranscriptStats:
    '''Scrape various values from run output into more accesible readout'''
    def __init__(self, paths, output_directory, instrument):

        print('\nGenerating summary files...')

        self.output_directory = output_directory

        # Init master slide df and master fov df to summarize entire run, slides, and fov in a single table
        self.master_slide_df = pd.DataFrame()
        self.master_fov_df = {}
        self.targ_obs = {}

        # Label files using path identifiers
        self.labeled = LabelFiles(file_paths = paths)

        # self.hexbin_plot(instrument)
        # self.color_conf_heat_map(instrument)
        # self.full_image_hist()
        # self.raw_plot()
        self.z_raw_plot()

    def parse_3d_reg(self, path):

        subfiles = [StringIO()]

        with open(path) as file:
            for line in file:
                if line.strip() == "": # blank line, new subfile                                                                                                                                       
                    subfiles.append(StringIO())
                else: # continuation of same subfile                                                                                                                                                   
                    subfiles[-1].write(line)
        table = {}
        for i, subfile in zip(range(len(subfiles)), subfiles):
            subfile.seek(0)
            table[i] = pd.read_csv(subfile, sep=',')

        df = table[0]
        x_index = df.Cycle[df.Cycle == 'dx'].index
        y_index = df.Cycle[df.Cycle == 'dy'].index

        x_global = df.iloc[x_index[0]][1]
        y_global = df.iloc[y_index[0]][1]

        return x_global, y_global

    def parse_jitter(self, path):
        subfiles = [StringIO()]

        with open(path) as file:
            for line in file:
                try:
                    if line.strip() == "": # blank line, new subfile                                                                                                                                       
                        subfiles.append(StringIO())
                    else: # continuation of same subfile                                                                                                                                                   
                        subfiles[-1].write(line)
                except:
                    continue

        table = {}
        for i, subfile in zip(range(len(subfiles)), subfiles):
            subfile.seek(0)
            table[i] = pd.read_csv(subfile, sep=',', on_bad_lines = 'skip', skiprows = [0])

        df = table[1]
        color_key = {1 : 'BB', 2 : 'GG', 3 : 'YY', 4 : 'RR'}
        for row in range(len(df)):
            df.at[row, 'Ch'] = color_key[df['Ch'][row]]
        df.index = df['Ch']
        df = df.drop(['Ch'], axis = 1)    

        return df

    def full_image_hist(self):
        # Create all output directories for outgoing plots and data 
        img_hist_path = self.output_directory + '\\Overlays\\Image_bounded_hist'
        img_hist_tab_path = self.output_directory + '\\Overlays\\Image_bounded_tabulated'

        if not os.path.exists(self.output_directory + '\\Overlays'):
            os.mkdir(self.output_directory + '\\Overlays')
        if not os.path.exists(img_hist_path):
            os.mkdir(img_hist_path)
        if not os.path.exists(img_hist_tab_path):
            os.mkdir(img_hist_tab_path)

        total_hist = pd.DataFrame()
        # Init spot cords nested dictionary dataframe to hold feather file spot coordinates 
        colors = ['BB', 'GG', 'YY', 'RR']
        for run in  self.labeled.proj:
            for slide in  self.labeled.proj[run]:
                for cycle in  self.labeled.proj[run][slide]:
                    for fov in  self.labeled.proj[run][slide][cycle]:
                        for rep in  self.labeled.proj[run][slide][cycle][fov]:
                            p = pathlib.PureWindowsPath( self.labeled.proj[run][slide][cycle][fov][rep])
                            max = skio.imread(p.as_posix())
                            # Split multichannel tiff file into seperate numpy arrays
                            # b=1
                            # y=1
                            # g=1
                            b, g, y, r = cv2.split(max)
                            color_dict = {'BB' : b, 'GG' : g, 'YY' : y, 'RR' : r}
                            color_thresh = {'BB' : 1500, 'GG' : 1000, 'YY' : 1250, 'RR' : 1000}
                            # For each color, plot xy raw spot cordinates
                            for color in colors:
                                if color in ['BB', 'GG', 'YY']:
                                    continue
                                lower_thresh = color_thresh[color]
                                vals = color_dict[color].flatten()
                                thresh_vals = vals[(vals > lower_thresh) & (vals < 3000)]

                                tab_hist = pd.DataFrame()
                                tab_hist['Pixel_intensity'] = thresh_vals.tolist()
                                # tab_hist.to_csv(img_hist_path + '\\{}_{}_{}_{}_{}_{}_full_image_bounded_hist.csv'.format(run, slide, cycle, fov, rep, color))

                                id_tag = '{}_{}_{}_{}_{}_'.format(run, slide, cycle, fov, rep, color)

                                total_hist.at[id_tag, 'Slide'] = slide 
                                total_hist.at[id_tag, 'Cycle'] = cycle 
                                total_hist.at[id_tag, 'FOV'] = fov 
                                total_hist.at[id_tag, 'Rep'] = rep 
                                total_hist.at[id_tag, 'Total_pixels'] = len(tab_hist) 
                                total_hist.to_csv(img_hist_tab_path + '\\Total_counts.csv')

                                fig, ax = plt.subplots(figsize = (15,10))

                                # ax.figure(figsize=(5,7))
                                ax.hist(thresh_vals, bins=40, alpha=0.5)
                                ax.set_xlabel("Intensity", size=14)
                                ax.set_ylabel("Count", size=14)
                                ratio = 0.1
                                ax.set_xlim(1000, 3000)
                                x_left, x_right = ax.get_xlim()
                                y_low, y_high = ax.get_ylim()
                                ax.set_aspect(abs((x_right-x_left)/(y_low-y_high))*ratio)
                                ax.axes.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
                                ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.1e'))

                                ax.set_title("{}_{}_{}_{}_{}_{}_ Full_image_pixel_values".format(run, slide, cycle, fov, rep, color))
                                ax.legend(loc='upper right')
                                plt.savefig(img_hist_path + '\\{}_{}_{}_{}_{}_{}_full_image_bounded_hist.png'.format(run, slide, cycle, fov, rep, color), bbox_inches = 'tight', pad_inches = 0)
                                print('Saving' + '\\{}_{}_{}_{}_{}_{}_full_image_bounded_hist.png'.format(run, slide, cycle, fov, rep, color))
                                plt.close()
                                                                
        return

    def z_raw_plot(self):
        # Create all output directories for outgoing plots and data 
        z_path = self.output_directory + '\\Overlays\\Z_slice_overlays'
        tab_z_intensity_path = self.output_directory + '\\Overlays\\Spot_intensity_zslice_tabulated'
        tab_z_hist_path = self.output_directory + '\\Overlays\\Spot_intensity_zslice_histogram'


        if not os.path.exists(self.output_directory + '\\Overlays'):
            os.mkdir(self.output_directory + '\\Overlays')
        if not os.path.exists(z_path):
            os.mkdir(z_path)
        if not os.path.exists(tab_z_intensity_path):
            os.mkdir(tab_z_intensity_path)
        if not os.path.exists(tab_z_hist_path):
            os.mkdir(tab_z_hist_path)

        # Init spot cords nested dictionary dataframe to hold feather file spot coordinates 
        colors = ['BB', 'GG', 'YY', 'RR']
        for run in  self.labeled.spot:
            for slide in  self.labeled.spot[run]:
                for cycle in  self.labeled.spot[run][slide]:
                    for fov in  self.labeled.spot[run][slide][cycle]:
                        for rep in  self.labeled.spot[run][slide][cycle][fov]:
                            x_global, y_global = self.parse_3d_reg(self.labeled.global_shift[run][slide][cycle][fov][rep])
                            channel_shift = self.parse_jitter(self.labeled.channel_shift[run][slide][cycle][fov][rep])
                            # Init holding data frame to combine spot coordinates for all zslices in a single fov
                            all_z_hist = {}
                            zimg_list = list(self.labeled.spot[run][slide][cycle][fov][rep])
                            zimg_list.sort(key=lambda x: int(''.join(filter(str.isdigit, x))))
                            for zslice in  zimg_list:
                                zimg_index = zimg_list.index(zslice)
                                zslice_fth = pd.DataFrame()
                                trim_str = str(self.labeled.spot[run][slide][cycle][fov][rep][zslice])
                                fth_df = pd.read_feather(trim_str)
                                # Feather files store coordinate values multiplied by 10 so we undo this operation to get original coordinates
                                fth_df['10X'] = fth_df['10X'].div(10)
                                fth_df['10Y'] = fth_df['10Y'].div(10)
                                fth_df = fth_df.reset_index(drop = True)
                                x = (fth_df['10X'].to_numpy()).astype(int)
                                y = (fth_df['10Y'].to_numpy()).astype(int)
                                # Init a list of cords then add x and y cords to each list member
                                cords = []
                                for i in range(0, len(x)):
                                    cord_arr = [x[i] - x_global, y[i] - y_global]
                                    cords.append(cord_arr.copy())
                                zslice_fth['cords'] = pd.Series(cords)
                                zslice_fth['color'] = fth_df['Call']       
                                zslice_fth['conf'] = fth_df['Conf']        
                                # Init stack_df if length is 0, otherwise just append new dataframe to end
                                zimg_index = 'Z' + str(zimg_index + 1).zfill(3)
                                # Open projected image for specific spot cords
                                zimg_path = pathlib.PureWindowsPath(self.labeled.z[run][slide][cycle][fov][rep][zimg_index])
                                max = skio.imread(zimg_path.as_posix())
                                # Split multichannel tiff file into seperate numpy arrays
                                b, g, y, r = cv2.split(max)
                                color_dict = {'BB' : b, 'GG' : g, 'YY' : y, 'RR' : r}
                                df_color = {'BB' : 11, 'GG' : 22, 'YY' : 33, 'RR' : 44}
                                intensity_dict = {}
                                # For each color, plot xy raw spot cordinates
                                for color in colors:
                                    # if color in ['BB', 'GG', 'YY']:
                                    #     continue
                                    x_channel = channel_shift['dx(abs)'].loc[color]
                                    y_channel = channel_shift['dy(abs)'].loc[color]                                    
                                    # Transpose image array to make compatible with numpy coordinate format
                                    color_dict[color] = cv2.transpose(color_dict[color])
                                    intensity_dict[color] = pd.DataFrame()
                                    # Contrast stretching
                                    original = np.copy(color_dict[color])
                                    p2, p98 = np.percentile(color_dict[color], (2, 98))
                                    color_dict[color] = skimage.exposure.rescale_intensity(color_dict[color], in_range=(p2, p98))
                                    # Turn image into RGB by merging 3 copies together
                                    color_dict[color] = cv2.merge([color_dict[color], color_dict[color], color_dict[color]])
                                    spot_cords = (zslice_fth[zslice_fth['color'] == df_color[color]]).reset_index(drop = True)
                                    if spot_cords.empty:
                                        continue
                                    for i in range(0, len(spot_cords['cords'])):
                                        x_cord = round(spot_cords['cords'][i][0] - x_channel)
                                        y_cord = round(spot_cords['cords'][i][1] - y_channel)
                                        # Green for high confidence spots
                                        if spot_cords['conf'][i] == 9:
                                            row, col= draw.circle_perimeter(x_cord, y_cord, 3, shape = color_dict[color] .shape)
                                            disk_row, disk_col = draw.disk((x_cord, y_cord), 3)
                                            intensity_dict[color].at[i, 'Conf'] = 9
                                            intensity_dict[color].at[i, 'Max_int'] = np.amax(original[disk_row, disk_col])
                                            color_dict[color][row, col] = (0, 65535, 0)
                                        # Blue for mid confidence spots
                                        elif spot_cords['conf'][i] == 7:
                                            row, col= draw.circle_perimeter(x_cord, y_cord, 3, shape = color_dict[color] .shape)
                                            disk_row, disk_col = draw.disk((x_cord, y_cord), 3)
                                            intensity_dict[color].at[i, 'Conf'] = 7
                                            intensity_dict[color].at[i, 'Max_int'] = np.amax(original[disk_row, disk_col])
                                            color_dict[color][row, col] = (25500, 25500, 65535)
                                        # Red for low confidence spots
                                        elif spot_cords['conf'][i] == 5:
                                            row, col= draw.circle_perimeter(x_cord, y_cord, 3, shape = color_dict[color] .shape)
                                            disk_row, disk_col = draw.disk((x_cord, y_cord), 3)
                                            intensity_dict[color].at[i, 'Conf'] = 5
                                            intensity_dict[color].at[i, 'Max_int'] = np.amax(original[disk_row, disk_col])
                                            color_dict[color][row, col] = (65535, 0, 0)
                                        else:
                                            continue

                                    # Transpose plotted image to match original
                                    color_dict[color] = np.rot90(color_dict[color], 1)
                                    color_dict[color] = np.flipud(color_dict[color])
                                    # Change image to 8-bit so that it can be combined with cell segmentation overlay

                                    print('Saving: {}_{}_{}_{}_{}_{}_{}_projected_channel_spot_overlay.tif'.format(run, slide, cycle, fov, rep, color, zslice))
                                    tiff.imwrite(z_path + '\\{}_{}_{}_{}_{}_{}_{}_projected_channel_spot_overlay.tif'.format(run, 
                                    slide, cycle, fov, rep, color, zslice), color_dict[color])

                                    print('Saving: {}_{}_{}_{}_{}_{}_{}_tabulated_intensity.csv'.format(run, slide, cycle, fov, rep, zslice, color))
                                    intensity_dict[color].to_csv(tab_z_intensity_path + '\\{}_{}_{}_{}_{}_{}_{}_tabulated_intensity.csv'.format(run, 
                                    slide, cycle, fov, rep, zslice, color))

                                    # Perform extensive garbage collection to prevent memory leaks
                                    color_dict[color] = None
                                    eight_bit = None
                                    original = None
                                    del color_dict[color]
                                    del eight_bit
                                    del original
                                    gc.collect()

                                    dummy_df = pd.DataFrame()
                                    if color not in all_z_hist:
                                        all_z_hist[color] = pd.concat([dummy_df, intensity_dict[color]], ignore_index = True)
                                    else:
                                        all_z_hist[color] = pd.concat([all_z_hist[color], intensity_dict[color]], ignore_index = True)
                                
                                for color in all_z_hist:
                                    df = all_z_hist[color]

                                    high = df['Max_int'][df['Conf'] == 9]
                                    mid = df['Max_int'][df['Conf'] == 7]
                                    low = df['Max_int'][df['Conf'] == 5]

                                    high = high.reset_index(drop = True)
                                    mid = mid.reset_index(drop = True)
                                    low = low.reset_index(drop = True)

                                    df = pd.DataFrame()
                                    df['High'] = high
                                    df['Mid'] = mid
                                    df['Low'] = low

                                    fig, ax = plt.subplots(figsize = (30,10))

                                    ax.hist(high, bins=200, alpha=0.5, label="High")
                                    ax.hist(mid, bins=200, alpha=0.5, label="Mid")
                                    ax.hist(low, bins=200, alpha=0.5, label="Low")
                                    ax.set_xlabel("Intensity", size=14)
                                    ax.set_ylabel("Count", size=14)
                                    ratio = 0.1
                                    ax.set_xlim(0, 4000)
                                    x_left, x_right = ax.get_xlim()
                                    y_low, y_high = ax.get_ylim()
                                    ax.set_aspect(abs((x_right-x_left)/(y_low-y_high))*ratio)
                                    ax.axes.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
                                    ax.yaxis.set_major_formatter(mtick.FormatStrFormatter('%.1e'))

                                    ax.set_title("{}_{}_{}_{}_{}_{}_max pixel spot intensity".format(run, slide, cycle, fov, rep, color))
                                    ax.legend(loc='upper right')
                                    plt.savefig(tab_z_hist_path + '\\{}_{}_{}_{}_{}_{}_confidence_hist_comp_allz.png'.format(run, slide, cycle, fov, rep, color), bbox_inches = 'tight', pad_inches = 0)
                                    plt.close()

        return

    def raw_plot(self):
        # Create all output directories for outgoing plots and data 
        bit16_path = self.output_directory + '\\Overlays\\16_bit_channel_only'
        bit8_path = self.output_directory + '\\Overlays\\8_bit_channel_only'
        tab_intensity_path = self.output_directory + '\\Overlays\\Tabulated_intensity'

        if not os.path.exists(self.output_directory + '\\Overlays'):
            os.mkdir(self.output_directory + '\\Overlays')
        if not os.path.exists(bit16_path):
            os.mkdir(bit16_path)
        if not os.path.exists(bit8_path):
            os.mkdir(bit8_path)
        if not os.path.exists(tab_intensity_path):
            os.mkdir(tab_intensity_path)

        # Init spot cords nested dictionary dataframe to hold feather file spot coordinates 
        spot_cords = {}
        colors = ['BB', 'GG', 'YY', 'RR']
        for run in  self.labeled.spot:
            spot_cords[run] = {}
            for slide in  self.labeled.spot[run]:
                spot_cords[run][slide] = {}
                for cycle in  self.labeled.spot[run][slide]:
                    spot_cords[run][slide][cycle] = {}
                    for fov in  self.labeled.spot[run][slide][cycle]:
                        spot_cords[run][slide][cycle][fov] = {}
                        # Read in cell segmentation overlay image at this stage in the dictionary nesting
                        if self.labeled.cell:
                            cell_path = pathlib.PureWindowsPath(self.labeled.cell[run][slide][fov])
                            cell = skio.imread(cell_path.as_posix())
                        for rep in  self.labeled.spot[run][slide][cycle][fov]:
                            spot_cords[run][slide][cycle][fov][rep] = {}
                            # Init holding data frame to combine spot coordinates for all zslices in a single fov
                            stack_df = pd.DataFrame()
                            for zslice in  self.labeled.spot[run][slide][cycle][fov][rep]:
                                spot_cords[run][slide][cycle][fov][rep][zslice] = {}
                                zslice_fth = pd.DataFrame()
                                trim_str = str( self.labeled.spot[run][slide][cycle][fov][rep][zslice])
                                fth_df = pd.read_feather(trim_str)
                                # Feather files store coordinate values multiplied by 10 so we undo this operation to get original coordinates
                                fth_df['10X'] = fth_df['10X'].div(10)
                                fth_df['10Y'] = fth_df['10Y'].div(10)
                                fth_df = fth_df.reset_index(drop = True)
                                x = (fth_df['10X'].to_numpy()).astype(int)
                                y = (fth_df['10Y'].to_numpy()).astype(int)
                                # Init a list of cords then add x and y cords to each list member
                                cords = []
                                for i in range(0, len(x)):
                                    cord_arr = [x[i], y[i]]
                                    cords.append(cord_arr.copy())
                                zslice_fth['cords'] = pd.Series(cords)
                                zslice_fth['color'] = fth_df['Call']       
                                zslice_fth['conf'] = fth_df['Conf']        
                                # Init stack_df if length is 0, otherwise just append new dataframe to end
                                if len(stack_df) != 0:
                                    stack_df = stack_df.append(zslice_fth, ignore_index = True)
                                else:
                                    stack_df = zslice_fth

                            spot_cords[run][slide][cycle][fov][rep]['BB'] = (stack_df[stack_df['color'] == 11]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['GG'] = (stack_df[stack_df['color'] == 22]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['YY'] = (stack_df[stack_df['color'] == 33]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['RR'] = (stack_df[stack_df['color'] == 44] ).reset_index(drop = True)
                            print('Finished compiling spot cords for: {}_{}_{}_{}_{}'.format(run, slide, fov, cycle, rep))
                            # Open projected image for specific spot cords
                            p = pathlib.PureWindowsPath(self.labeled.proj[run][slide][cycle][fov][rep])
                            max = skio.imread(p.as_posix())
                            # Split multichannel tiff file into seperate numpy arrays
                            b, g, y, r = cv2.split(max)
                            color_dict = {'BB' : b, 'GG' : g, 'YY' : y, 'RR' : r}
                            intensity_dict = {}
                            # For each color, plot xy raw spot cordinates
                            for color in colors:
                                if color in ['BB', 'GG', 'YY']:
                                    continue
                                # Transpose image array to make compatible with numpy coordinate format
                                color_dict[color] = cv2.transpose(color_dict[color])
                                intensity_dict[color] = pd.DataFrame()
                                # Contrast stretching
                                original = np.copy(color_dict[color])
                                original, background = subtract_background_rolling_ball(original, 50, light_background = False, use_paraboloid=False, do_presmooth=True)
                                adj_orginal = original - background
                                p2, p98 = np.percentile(color_dict[color], (2, 98))
                                color_dict[color] = skimage.exposure.rescale_intensity(color_dict[color], in_range=(p2, p98))
                                # Turn image into RGB by merging 3 copies together
                                color_dict[color] = cv2.merge([color_dict[color], color_dict[color], color_dict[color]])
                                for i in range(0, len(spot_cords[run][slide][cycle][fov][rep][color]['cords'])):
                                    # Green for high confidence spots
                                    if spot_cords[run][slide][cycle][fov][rep][color]['conf'][i] == 9:
                                        row, col= draw.circle_perimeter(spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0], spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1], 3, shape = color_dict[color] .shape)
                                        disk_row, disk_col = draw.disk((spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0], spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1]), 2)
                                        intensity_dict[color].at[i, 'Conf'] = 9
                                        intensity_dict[color].at[i, 'Max_int'] = np.amax(adj_orginal[disk_row, disk_col])
                                        color_dict[color][row, col] = (0, 65535, 0)
                                    # Blue for mid confidence spots
                                    elif spot_cords[run][slide][cycle][fov][rep][color]['conf'][i] == 7:
                                        row, col= draw.circle_perimeter(spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0], spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1], 3, shape = color_dict[color] .shape)
                                        disk_row, disk_col = draw.disk((spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0], spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1]), 2)
                                        intensity_dict[color].at[i, 'Conf'] = 7
                                        intensity_dict[color].at[i, 'Max_int'] = np.amax(adj_orginal[disk_row, disk_col])
                                        color_dict[color][row, col] = (25500, 25500, 65535)
                                    # Red for low confidence spots
                                    elif spot_cords[run][slide][cycle][fov][rep][color]['conf'][i] == 5:
                                        row, col= draw.circle_perimeter(spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0], spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1], 3, shape = color_dict[color] .shape)
                                        disk_row, disk_col = draw.disk((spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0], spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1]), 2)
                                        intensity_dict[color].at[i, 'Conf'] = 5
                                        intensity_dict[color].at[i, 'Max_int'] = np.amax(adj_orginal[disk_row, disk_col])
                                        color_dict[color][row, col] = (65535, 0, 0)
                                    else:
                                        continue

                                # Transpose plotted image to match original
                                color_dict[color] = np.rot90(color_dict[color], 1)
                                color_dict[color] = np.flipud(color_dict[color])
                                # Change image to 8-bit so that it can be combined with cell segmentation overlay
                                eight_bit = (color_dict[color]/256).astype('uint8')
                                alpha = 0.4
                                beta = 1.0 - alpha

                                print('Saving: {}_{}_{}_{}_{}_{}_projected_channel_spot_overlay.tif'.format(run, slide, cycle, fov, rep, color))
                                tiff.imwrite(bit16_path + '\\{}_{}_{}_{}_{}_{}_projected_channel_spot_overlay.tif'.format(run, 
                                slide, cycle, fov, rep, color), color_dict[color])
                                
                                if self.labeled.cell:
                                    alpha_blend = cv2.addWeighted(cell, alpha, eight_bit, beta, 0.0)
                                    print('Saving: {}_{}_{}_{}_{}_{}_cell_spot_overlay.tif'.format(run, slide, cycle, fov, rep, color))
                                    tiff.imwrite(bit8_path + '\\{}_{}_{}_{}_{}_{}_cell_spot_overlay.tif'.format(run, 
                                    slide, cycle, fov, rep, color), alpha_blend)

                                print('Saving: {}_{}_{}_{}_{}_{}_tabulated_intensity.csv'.format(run, slide, cycle, fov, rep, color))
                                intensity_dict[color].to_csv(tab_intensity_path + '\\{}_{}_{}_{}_{}_{}_tabulated_intensity.csv'.format(run, 
                                slide, cycle, fov, rep, color))

                                # grouped_intensity = intensity_dict[color].groupby('Conf')
                                df = intensity_dict[color]
                                high = df['Max_int'][df['Conf'] == 9]
                                mid = df['Max_int'][df['Conf'] == 7]
                                low = df['Max_int'][df['Conf'] == 5]

                                high = high.reset_index(drop = True)
                                mid = mid.reset_index(drop = True)
                                low = low.reset_index(drop = True)

                                df = pd.DataFrame()
                                df['High'] = high
                                df['Mid'] = mid
                                df['Low'] = low

                                fig, ax = plt.subplots(figsize = (30,10))

                                # ax.figure(figsize=(5,7))
                                ax.hist(high, bins=200, alpha=0.5, label="High")
                                ax.hist(mid, bins=200, alpha=0.5, label="Mid")
                                ax.hist(low, bins=200, alpha=0.5, label="Low")
                                ax.set_xlabel("Intensity", size=14)
                                ax.set_ylabel("Count", size=14)
                                ratio = 0.1
                                # ax.set_xlim(1000, 5000)
                                x_left, x_right = ax.get_xlim()
                                y_low, y_high = ax.get_ylim()
                                ax.set_aspect(abs((x_right-x_left)/(y_low-y_high))*ratio)

                                ax.set_title("{}_{}_{}_{}_{}_{}_max pixel spot intensity".format(run, slide, cycle, fov, rep, color))
                                ax.legend(loc='upper right')
                                plt.savefig(tab_intensity_path + '\\{}_{}_{}_{}_{}_{}_confidence_hist_comp.png'.format(run, slide, cycle, fov, rep, color), bbox_inches = 'tight', pad_inches = 0)
                                plt.close()

                                # Perform extensive garbage collection to prevent memory leaks
                                color_dict[color] = None
                                spot_cords[run][slide][cycle][fov][rep][color] = None
                                eight_bit = None
                                original = None
                                del color_dict[color]
                                del spot_cords[run][slide][cycle][fov][rep][color]
                                del eight_bit
                                del original
                                gc.collect()

        return

    def hexbin_plot(self, instrument):
        # Create all output directories for outgoing plots and data 
        density_path = self.output_directory + '\\Overlays\\Hex_density'
        tab_density_path = self.output_directory + '\\Overlays\\Tabulated_density'

        if not os.path.exists(self.output_directory + '\\Overlays'):
            os.mkdir(self.output_directory + '\\Overlays')
        if not os.path.exists(density_path):
            os.mkdir(density_path)
        if not os.path.exists(tab_density_path):
            os.mkdir(tab_density_path)

        
        # Init spot cords nested dictionary dataframe to hold feather file spot coordinates 
        spot_cords = {}
        colors = ['BB', 'GG', 'YY', 'RR']
        for run in  self.labeled.spot:
            spot_cords[run] = {}
            for slide in  self.labeled.spot[run]:
                spot_cords[run][slide] = {}
                for cycle in  self.labeled.spot[run][slide]:
                    spot_cords[run][slide][cycle] = {}
                    for fov in  self.labeled.spot[run][slide][cycle]:
                        spot_cords[run][slide][cycle][fov] = {}
                        for rep in  self.labeled.spot[run][slide][cycle][fov]:
                            spot_cords[run][slide][cycle][fov][rep] = {}
                            # Init holding data frame to combine spot coordinates for all zslices in a single fov
                            stack_df = pd.DataFrame()
                            for zslice in  self.labeled.spot[run][slide][cycle][fov][rep]:
                                spot_cords[run][slide][cycle][fov][rep][zslice] = {}
                                zslice_fth = pd.DataFrame()
                                trim_str = str( self.labeled.spot[run][slide][cycle][fov][rep][zslice])
                                fth_df = pd.read_feather(trim_str)
                                # Feather files store coordinate values multiplied by 10 so we undo this operation to get original coordinates
                                fth_df['10X'] = fth_df['10X'].div(10)
                                fth_df['10Y'] = fth_df['10Y'].div(10)
                                fth_df = fth_df.reset_index(drop = True)
                                x = (fth_df['10X'].to_numpy()).astype(int)
                                y = (fth_df['10Y'].to_numpy()).astype(int)
                                # Init a list of cords then add x and y cords to each list member
                                cords = []
                                for i in range(0, len(x)):
                                    cord_arr = [x[i], y[i]]
                                    cords.append(cord_arr.copy())
                                # zslice_fth['X'] = fth_df['10X']
                                # zslice_fth['Y'] = fth_df['10Y']

                                # cords_flip = np.flipud(cords)
                                cords_flip = np.fliplr(cords)
                                xy = y -x
                                cords_rot = np.empty(cords_flip.shape)
                                for element_num in range(len(cords_flip)):
                                    cords_rot[element_num] = [cords_flip[element_num][1], -cords_flip[element_num][0]] 


                                # zslice_fth['cords'] = cords_flip.tolist()
                                zslice_fth['X'] = cords_rot[:,0].tolist()
                                zslice_fth['Y'] = cords_rot[:,1].tolist()
                                zslice_fth['color'] = fth_df['Call']       
                                zslice_fth['conf'] = fth_df['Conf']        
                                # Init stack_df if length is 0, otherwise just append new dataframe to end
                                if len(stack_df) != 0:
                                    stack_df = stack_df.append(zslice_fth, ignore_index = True)
                                else:
                                    stack_df = zslice_fth

                            spot_cords[run][slide][cycle][fov][rep]['BB'] = (stack_df[stack_df['color'] == 11]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['GG'] = (stack_df[stack_df['color'] == 22]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['YY'] = (stack_df[stack_df['color'] == 33]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['RR'] = (stack_df[stack_df['color'] == 44] ).reset_index(drop = True)
                            print('Finished compiling spot cords for: {}_{}_{}_{}_{}'.format(run, slide, fov, cycle, rep))

                            for color in colors:
                                if instrument == 'Alpha':
                                    figure(figsize=(10, 10), dpi=100)
                                elif instrument == 'Dash':
                                    figure(figsize=(20, 10), dpi=100)
                                x = spot_cords[run][slide][cycle][fov][rep][color]['X']
                                y = spot_cords[run][slide][cycle][fov][rep][color]['Y']
                                plt.hexbin(x, y, gridsize=70, cmap='inferno')
                                plt.axis('equal')
                                plt.title(run + '_' + slide + '_' + cycle + '_' + fov + '_' + rep + '_' + color + '_Hex_Density')
                                plt.colorbar(label='Density_SpotsPerHex')
                                plt.savefig(density_path + '\\' + run + '_' + slide + '_' + cycle + '_' + fov + '_' + rep + '_' + color + '.png', bbox_inches='tight')
                                plt.close()

    def color_conf_heat_map(self, instrument):

        def blockshaped(arr, nrows, ncols):
            h, w = arr.shape
            assert h % nrows == 0, "{} rows is not evenly divisble by {}".format(h, nrows)
            assert w % ncols == 0, "{} cols is not evenly divisble by {}".format(w, ncols)
            return (arr.reshape(h//nrows, nrows, -1, ncols)
                .swapaxes(1,2)
                .reshape(-1, nrows, ncols)) 

        # Create all output directories for outgoing plots and data 
        density_path = self.output_directory + '\\Overlays\\Hex_density'
        tab_density_path = self.output_directory + '\\Overlays\\Tabulated_density'

        if not os.path.exists(self.output_directory + '\\Overlays'):
            os.mkdir(self.output_directory + '\\Overlays')
        if not os.path.exists(density_path):
            os.mkdir(density_path)
        if not os.path.exists(tab_density_path):
            os.mkdir(tab_density_path)

        # Init spot cords nested dictionary dataframe to hold feather file spot coordinates 
        spot_cords = {}
        colors = ['BB', 'GG', 'YY', 'RR']
        for run in  self.labeled.spot:
            spot_cords[run] = {}
            for slide in  self.labeled.spot[run]:
                spot_cords[run][slide] = {}
                for cycle in  self.labeled.spot[run][slide]:
                    spot_cords[run][slide][cycle] = {}
                    for fov in  self.labeled.spot[run][slide][cycle]:
                        spot_cords[run][slide][cycle][fov] = {}
                        for rep in  self.labeled.spot[run][slide][cycle][fov]:
                            spot_cords[run][slide][cycle][fov][rep] = {}
                            # Init holding data frame to combine spot coordinates for all zslices in a single fov
                            stack_df = pd.DataFrame()
                            for zslice in  self.labeled.spot[run][slide][cycle][fov][rep]:
                                spot_cords[run][slide][cycle][fov][rep][zslice] = {}
                                zslice_fth = pd.DataFrame()
                                trim_str = str( self.labeled.spot[run][slide][cycle][fov][rep][zslice])
                                fth_df = pd.read_feather(trim_str)
                                # Feather files store coordinate values multiplied by 10 so we undo this operation to get original coordinates
                                fth_df['10X'] = fth_df['10X'].div(10)
                                fth_df['10Y'] = fth_df['10Y'].div(10)
                                fth_df = fth_df.reset_index(drop = True)
                                x = (fth_df['10X'].to_numpy()).astype(int)
                                y = (fth_df['10Y'].to_numpy()).astype(int)
                                # Init a list of cords then add x and y cords to each list member
                                cords = []
                                for i in range(0, len(x)):
                                    cord_arr = [x[i], y[i]]
                                    cords.append(cord_arr.copy())
                                # zslice_fth['X'] = fth_df['10X']
                                # zslice_fth['Y'] = fth_df['10Y']

                                # cords_flip = np.flipud(cords)
                                cords_flip = np.fliplr(cords)
                                xy = y -x
                                cords_rot = np.empty(cords_flip.shape)
                                for element_num in range(len(cords_flip)):
                                    cords_rot[element_num] = [cords_flip[element_num][1], -cords_flip[element_num][0]] 


                                # zslice_fth['cords'] = cords_flip.tolist()
                                zslice_fth['X'] = cords_rot[:,0].tolist()
                                zslice_fth['Y'] = cords_rot[:,1].tolist()
                                zslice_fth['color'] = fth_df['Call']       
                                zslice_fth['conf'] = fth_df['Conf']        
                                # Init stack_df if length is 0, otherwise just append new dataframe to end
                                if len(stack_df) != 0:
                                    stack_df = stack_df.append(zslice_fth, ignore_index = True)
                                else:
                                    stack_df = zslice_fth

                            spot_cords[run][slide][cycle][fov][rep]['BB'] = (stack_df[stack_df['color'] == 11]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['GG'] = (stack_df[stack_df['color'] == 22]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['YY'] = (stack_df[stack_df['color'] == 33]).reset_index(drop = True)
                            spot_cords[run][slide][cycle][fov][rep]['RR'] = (stack_df[stack_df['color'] == 44] ).reset_index(drop = True)
                            print('Finished compiling spot cords for: {}_{}_{}_{}_{}'.format(run, slide, fov, cycle, rep))

                            conf_arr = {}
                            for color in spot_cords[run][slide][cycle][fov][rep]:
                                if color not in conf_arr:
                                    conf_arr[color] = np.empty(4480, 4480)
                                if spot_cords[run][slide][cycle][fov][rep][color].empty:
                                    continue
                                for i in range(0, len(spot_cords[run][slide][cycle][fov][rep][color]['cords'])):
                                    x_cord = round(spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][0])
                                    y_cord = round(spot_cords[run][slide][cycle][fov][rep][color]['cords'][i][1])
                                    conf = spot_cords[run][slide][cycle][fov][rep][color]['conf'][i]
                                    if conf == 9:
                                        conf_arr[x_cord, y_cord] = 3
                                    elif conf == 7:
                                        conf_arr[x_cord, y_cord] = 2
                                    elif conf == 5:
                                        conf_arr[x_cord, y_cord] = 1
                                    else:
                                        continue


                            for color in colors:
                                if instrument == 'Alpha':
                                    figure(figsize=(10, 10), dpi=100)
                                elif instrument == 'Dash':
                                    figure(figsize=(20, 10), dpi=100)
                                x = spot_cords[run][slide][cycle][fov][rep][color]['X']
                                y = spot_cords[run][slide][cycle][fov][rep][color]['Y']
                                plt.hexbin(x, y, gridsize=70, cmap='inferno')
                                plt.axis('equal')
                                plt.title(run + '_' + slide + '_' + cycle + '_' + fov + '_' + rep + '_' + color + '_Hex_Density')
                                plt.colorbar(label='Density_SpotsPerHex')
                                plt.savefig(density_path + '\\' + run + '_' + slide + '_' + cycle + '_' + fov + '_' + rep + '_' + color + '.png', bbox_inches='tight')
                                plt.close()

if __name__ == "__main__":

    def process(path, instrument):
        run_regex = re.compile(r'Run\d{4}A|Run\d{4}|RunA\d{4}|RunB\d{4}|A\d{4}|R\d{4}')
        print('\nCompiling all file paths for *****{}*****'.format(path))
        run_label = run_regex.search(path).group()
        # Compile file paths for each run directory
        all_files = FindFiles(input_directory = path)
        # Scrape for protein information or transcript information
        ScrapeTranscriptStats(paths = all_files.paths, output_directory = output_directory, instrument = instrument)        
        if not new_run_list:
            new_run_list = [x for x in run_list if x != path]
        else:
            new_run_list = [x for x in new_run_list if x != path]
        processed_df = pd.DataFrame(new_run_list, columns = ['Origin_path'])
        # processed_df.to_excel(r'C:\Users\SMIT-1\Desktop\Colton\Spot_density_analysis\Query.xlsx')

    connection = 'dummy_string'

    output_directory = r'C:\Users\crobbins\Desktop\Hex_test'
    run_list_df = pd.read_excel(output_directory + '\\Query.xlsx')

    run_list = list(set(run_list_df['Origin_path']))
    drive_re_fun = re.compile(r'(?<=\\\\).*?(?=\\)')

    new_run_list = []

    for row in range(len(run_list_df)):
        path = run_list_df['Origin_path'][row]
        instrument = run_list_df['Instrument'][row]
        if connection not in path:
            try:
                connection = drive_re_fun.search(path).group()
                try:
                    wnet_connect('{}'.format(connection), 'project', 'charlie13')
                except:
                    try:
                        wnet_connect('{}'.format(connection), 'Nsadmin', 'BetaBetaBeta!')
                    except:
                        pass
                process(path, instrument)
            except AttributeError:
                try:
                    process(path, instrument)
                except:
                    print('Could not find path input')


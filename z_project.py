'''Funtion to plot spots from SMI run data'''

from numpy import dstack
import pandas as pd
import time
import PySimpleGUI as sg
import nd2
import os
import tifffile as tiff
from skimage import io
import numpy as np


class ProcessNd2:

    def __init__(self, input_directory):
        
        print('Commencing tif conversion...')

        self.start_time = time.time()
        self.input_directory = input_directory
        self.output_directory = input_directory

        self.all_nd2_files = self.__find_all_files()
        print("Files_found_finished --- %s seconds ---" % (time.time() - self.start_time))
        self.start_time = time.time()

        self.nd2_file_paths = self.__label_nd2_paths()
        print("nd2_files_labled --- %s seconds ---" % (time.time() - self.start_time))
        self.start_time = time.time()

        self.convert = self.__convert_dash_format()


    def __find_all_files(self):
        
        '''Locates all file paths recursively with specified entension in a user chosen directory
    
        Parameters
        ----------
        ent: string
            Target file entension
    
        Returns
        -------
        List of file paths in chosen directory
        '''

        import pathlib

        # Load in tif file mask paths

        input_path = pathlib.Path(self.input_directory)
        nd2_files = list()
        for filepath in input_path.rglob('*' + '.nd2'):
            if filepath.is_file():
                nd2_files.append(filepath)

        return nd2_files


    def __label_nd2_paths(self):
        '''Labels each file path in list with conserved label for parralel accession
    
        Parameters
        ----------
        files: list of paths
            List of file paths

        TODO: Come up with a regen scheme to remove leading and trailing strings

        Returns
        -------
        Dictionary of file name labels paried to their respective full paths
        '''
       
        import re

        # Label path dictionary with fov name labels 

        lead_str = 'Images\\'
        lag_str = '.nd2'
        target_regex = r'(?<=' + re.escape(lead_str) + r')(.*?)(?=' + re.escape(lag_str) + r')'
        label_func = re.compile(target_regex)

        nd2_file_paths = {}
        for nd2_path in self.all_nd2_files:
            str_path = str(nd2_path)
            fov = label_func.search(str_path)
            fov = fov.group()
            nd2_file_paths[fov] = nd2_path

        return nd2_file_paths

    def __convert_dash_format(self):



        for fov in self.nd2_file_paths:
            img = nd2.imread(self.nd2_file_paths[fov])

            five_channel_list = list()
            for x in range(5):
                project_list = list()
                for i in range(0, len(img)):
                    project_list.append(img[i][x])
                max_channel = np.max(project_list, axis = 0)
                five_channel_list.append(max_channel)
            five_channel_max = np.stack(five_channel_list)
            print('Saving: {}_MAX'.format(fov))
            tiff.imwrite(self.output_directory + '\\' + 'Max_' + fov + '.tif', five_channel_max)


class Input():
    def __init__(self):
        self.directory = self.get_params()

    def get_params(self):
        sg.ChangeLookAndFeel('LightBrown10')

        layout = [
            [sg.Text('ND2 to DASH format', size=(30, 1), font=("Helvetica", 25))],
            [sg.Text('Enter input path that includes subdirectory titled "Raw_images" that holds ND2 files', size=(80, 1), font=("Helvetica", 10))],       
            [sg.Text('Run DevApp counter after ND2 files are converted', size=(80, 1), font=("Helvetica", 10))],       
            [sg.Text('Run Nikon spot plotter after DevApp counter', size=(80, 1), font=("Helvetica", 10))],       
            [sg.Text('_' * 80)],
            [sg.Text('Enter in input path then press "Exit"', size=(35, 1))],
            [sg.Text('Input directory', size=(15, 1), auto_size_text=False, justification='right'),
            sg.InputText('C:\\Users\\user_name\\path_to_data', key='folder', do_not_clear=True), sg.FolderBrowse()],
            [sg.Button('Exit'),
            sg.Text(' ' * 40), sg.Button('SaveSettings'), sg.Button('LoadSettings')]
        ]

        window = sg.Window('Enter in plotting parameters', default_element_size=(40, 1), grab_anywhere=False)

        window.Layout(layout)

        while True:
            event, values = window.Read()

            if event == 'SaveSettings':
                filename = sg.PopupGetFile('Save Settings', save_as=True, no_window=True)
                window.SaveToDisk(filename)
                # save(values)
            elif event == 'LoadSettings':
                filename = sg.PopupGetFile('Load Settings', no_window=True)
                window.LoadFromDisk(filename)
                # load(form)
            elif event in ('Exit', None):
                break

        window.CloseNonBlocking() 


        return values['folder']

def main():
    

    input = Input()

    ProcessNd2(input_directory = input.directory)


if __name__ == '__main__':
    main()


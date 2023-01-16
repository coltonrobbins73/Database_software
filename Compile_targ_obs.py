import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pathlib
import re
import os
from scipy.stats import describe
import scikit_posthocs as sp

class FindFiles:
    '''Generates a list of file paths that match key words'''
    def __init__(self, input_directory):
        self.input_directory = input_directory
        self.paths = self.__find_files()
    def __find_files(self):
        
        paths = {}
        for root, dirs, files in os.walk(self.input_directory, topdown=False):
            for name in files:
                paths[root + '\\' + name] = name

        return paths

def split_by(df):

    analysis_df = pd.DataFrame()

    df = df.drop(['Unnamed: 0', 'Unnamed: 1'], axis = 1)

    gp = df.groupby(['Run'])
    mean_df = df.groupby(['Run']).mean()

    spots = [df.columns[2], df.columns[3], df.columns[4], df.columns[5]]

    # res = pd.concat([pd.DataFrame(describe(g[x]) for _, g in gp)\
    #                 .reset_index().assign(cat=x).set_index(['cat', 'index']) \
    #                 for x in spots], axis=0)

    gene = df['target'][1]

    for spot in spots:
        spot_array = []
        for group in gp:
            spot_array.append(list(group[1][spot]))
        if len(spot_array) == 1:
            continue
        pairwise_pval_df = sp.posthoc_dunn(spot_array, p_adjust = 'holm')
        var_ratio = (((pairwise_pval_df.to_numpy().flatten() >= 0.05).sum() - pairwise_pval_df.shape[0]) / ((pairwise_pval_df.shape[0]**2) - pairwise_pval_df.shape[0])) * 100
        analysis_df.at[gene, spot] = var_ratio

    return analysis_df, mean_df


def split_by_gene(paths):

    analysis_df = pd.DataFrame()
    mean_df = pd.DataFrame()
    for path in paths.paths:
        if '_' in paths.paths[path]:
            df = pd.read_csv(path)
            df = df.drop(['Unnamed: 0', 'Unnamed: 1'], axis = 1)

            if mean_df.empty:
                mean_df = df.groupby(['Run']).mean()
                gene = df['target'][1]
                run_list = [run_string + '_' + gene for run_string in mean_df.index] 
                mean_df.index = run_list                   
            else:
                mean_to_concat = df.groupby(['Run']).std()
                gene = df['target'][1]
                run_list = [run_string + gene for run_string in mean_to_concat.index] 
                mean_to_concat.index = run_list    
                mean_df = pd.concat([mean_df, mean_to_concat])


            # gp = df.groupby(['Run'])
            # spots = [df.columns[2], df.columns[3], df.columns[4], df.columns[5]]

            # # res = pd.concat([pd.DataFrame(describe(g[x]) for _, g in gp)\
            # #                 .reset_index().assign(cat=x).set_index(['cat', 'index']) \
            # #                 for x in spots], axis=0)

            # gene = df['target'][1]

            # for spot in spots:
            #     spot_array = []
            #     for group in gp:
            #         spot_array.append(list(group[1][spot]))
            #     if len(spot_array) == 1:
            #         continue
            #     pairwise_pval_df = sp.posthoc_dunn(spot_array, p_adjust = 'holm')
            #     var_ratio = (((pairwise_pval_df.to_numpy().flatten() >= 0.05).sum() - pairwise_pval_df.shape[0]) / ((pairwise_pval_df.shape[0]**2) - pairwise_pval_df.shape[0])) * 100
            #     analysis_df.at[gene, spot] = var_ratio
            
    mean_df.to_csv('K:\\Targ_obs_mean\\Compare_std_along_run.csv')
    # analysis_df.to_csv('K:\\Targ_obs_comparison\\Compare_along_run.csv')


class CombineData():
    def __init__(self, paths):

        gene_list = []
        for path in paths.paths:
            gene_list.append(re.split('-|\.',paths.paths[path])[1])

        gene_concat = {}
        for gene in gene_list:
            if gene not in gene_concat:
                gene_concat[gene] = pd.DataFrame()
            for path in paths.paths:
                if gene in paths.paths[path]:
                    df = pd.read_csv(path, engine = 'pyarrow')
                    gene_concat[gene] = pd.concat([gene_concat[gene], df], ignore_index = True)

            gene_concat[gene].to_csv('K:\\Targ_obs_concat\\{}.csv'.format(gene))        

if __name__ == "__main__":

    

    paths = FindFiles(input_directory = 'K:\\Targ_obs_concat')

    CombineData(paths)
    run_list = []
    for path in paths.paths:
        run_list.append(paths.paths[path].split('-')[0])
    run_list = list(set(run_list))
    for run in run_list:
        concat_df = pd.DataFrame()
        for path in paths.paths:
            if run in paths.paths[path]:
                df = pd.read_csv(path)
                single_gene_analysis_df = split_by(df)
                concat_df, mean_df = pd.concat([concat_df, single_gene_analysis_df])

    # split_by_gene(paths)


'''
Try a QQ plot of some of the genes after log normalization
both for individual spots and total spot obs

'''
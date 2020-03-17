import pandas as pd
import os
import json
import glob
import numpy as np
import doctest
import datetime as dt
import geopandas as gpd
import matplotlib.pyplot as plt
from geopandas import GeoSeries
from shapely.geometry import Polygon
import sys
import seaborn as sns

from etl import *
from process import *

cfg = json.load(open('../config/data-params.json'))
get_data(**cfg)
df1 = make_df(**cfg)
owd = os.getcwd()
os.chdir("../data/raw")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
all_filenames = np.sort(all_filenames)
newnames = all_filenames[:4]
pr2018 = pd.concat([pd.read_csv(f) for f in newnames ]).reset_index(drop = True)
pr2018['year'] = pr2018['date_stop'].astype(str).str[:4]
pr2018['year'] = pr2018['year'].astype(int)
pr2018 = pr2018[wanted_columns_pre]

consent = pd.read_csv(all_filenames[5])[['stop_id', 'consented']]
contra = pd.read_csv(all_filenames[6])[['stop_id', 'contraband']]
seiz = pd.read_csv(all_filenames[7])[['stop_id', 'type_of_property_seized']]
race = pd.read_csv(all_filenames[8])[['stop_id', 'race']]
search = pd.read_csv(all_filenames[9])[['stop_id', 'basis_for_search']]
whystop = pd.read_csv(all_filenames[10])[['stop_id', 'reason_for_stop']]
stopresult = pd.read_csv(all_filenames[11])[['stop_id', 'result']]

pt2018 = pd.read_csv(all_filenames[4])
pt2018 = pt2018.merge(consent, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018 = pt2018.merge(contra, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018 = pt2018.merge(seiz, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018 = pt2018.merge(race, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018 = pt2018.merge(search, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018 = pt2018.merge(whystop, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018 = pt2018.merge(stopresult, on='stop_id')
pt2018 = pt2018.drop_duplicates(subset = 'stop_id')
pt2018['year'] = pt2018['date_stop'].astype(str).str[:4]
pt2018['year'] = pt2018['year'].astype(int)

pre2018 = pre18(pr2018)
pre2018 = clean_time(pre2018)
post2018 = post18(pt2018)
post2018 = clean_time(post2018)
allyears = pd.concat([pre2018, post2018], axis = 0).reset_index(drop=True)

getdir = os.getcwd()
os.chdir(owd)

if not os.path.exists("../data/out"):
    os.mkdir("../data/out")
#tables.to_csv("%s/%s.csv"%(outpath, year))
allyears.to_csv("../data/out/combined.csv")
pre2018.to_csv("../data/out/pre2018.csv")
post2018.to_csv("../data/out/post2018.csv")

def censusyr(df):   
    racepops = df[['beat', 'div', 'serv', 'H7X001', 'H7X002', 'H7X003', 'H7X004', 'H7X005', 'H7X006', 'H7X007', 'H7X008']]
    racepops['Other'] = racepops['H7X007']+ racepops['H7X008']
    racepops = racepops[['beat', 'div', 'serv', 'H7X001', 'H7X002', 'H7X003', 'H7X004', 'H7X005', 'H7X006', 'Other']]
    racepops = racepops.rename(columns = {'beat': 'beat', 'div':'div', 'serv':'serv', 'H7X001':'Total', 'H7X002': 'White', 'H7X003': 'Black/African American', 
                        'H7X004': 'Native American', 'H7X005':'Asian', 'H7X006': 
                        'Pacific Islander', 'Other': 'Other'})
    return racepops

def yearcnt(df):
    iht = pd.pivot_table(df, index= ['service_area'], columns = ['subject_race'], aggfunc={'stop_id': 'count'})
    iht = iht.dropna()
    iht[ ('stop_id',                         'Other')] = iht[ ('stop_id',                         'Other')] + iht[ ('stop_id',                         'Hispanic/Latino/a')] + iht[('stop_id', 'Middle Eastern or South Asian')]
    iht = iht.drop(columns = [('stop_id', 'Middle Eastern or South Asian'), ('stop_id',             'Hispanic/Latino/a')])
    iht.columns = iht.columns.droplevel(0)
    iht['Total'] = list(iht.sum(axis=1))
    return iht

def light_or_dark(df):
    lst = []
    for i in range(len(df)):
        if df.times[i] > df.time_stop[i]:
            lst.append('light')
        else:
            lst.append('dark')
    return lst

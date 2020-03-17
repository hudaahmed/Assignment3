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

cfg = json.load(open('config/data-params.json'))
get_data(**cfg)
df1 = make_df(**cfg)
owd = os.getcwd()
os.chdir("data/raw")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
all_filenames = np.sort(all_filenames)

pre2018 = pre18(createtables(all_filenames)[0])
pre2018 = clean_time_pre(pre2018)
post2018 = post18(createtables(all_filenames)[1])
post2018 = clean_time_post(post2018)
allyears = pd.concat([pre2018, post2018], axis = 0).reset_index(drop=True)

getdir = os.getcwd()
os.chdir(owd)

if not os.path.exists("data/out"):
    os.mkdir("data/out")
#tables.to_csv("%s/%s.csv"%(outpath, year))
allyears.to_csv("data/out/combined.csv")
pre2018.to_csv("data/out/pre2018.csv")
post2018.to_csv("data/out/post2018.csv")

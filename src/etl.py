
import os
import pandas as pd
import json
import glob
import numpy as np
import doctest
import datetime as dt
import geopandas as gpd


def get_data(years, components, outpath):
    web = 'http://seshat.datasd.org/pd/vehicle_stops_'
    for year in years:
        if year < 2018:
            url = web + str(year) + '_datasd_v1.csv'
            tables = pd.read_csv(url)
            if not os.path.exists(outpath):
                os.mkdir(outpath)
            tables.to_csv("%s/%s.csv"%(outpath, year))
        else:
            url = 'http://seshat.datasd.org/pd/ripa_stops_datasd_v1.csv'
            tables = pd.read_csv(url)
            if not os.path.exists(outpath):
                os.mkdir(outpath)
            tables.to_csv("%s/%s.csv"%(outpath, year))
            
    for comps in components:
        url = 'http://seshat.datasd.org/pd/ripa_' + comps + '_datasd.csv'
        tables = pd.read_csv(url)
        if not os.path.exists(outpath):
            os.mkdir(outpath)
        tables.to_csv("%s/%s.csv"%(outpath, comps))
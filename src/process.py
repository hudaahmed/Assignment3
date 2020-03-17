
import os
import pandas as pd
import json
import glob
import numpy as np
import doctest
import datetime as dt
import geopandas as gpd
        
        
def make_df(years, components, outpath): 
    df = pd.DataFrame()
    for i in years:
        if i < 2018:
            path = '../data/raw/' + str(i) + '.csv'
            table = pd.read_csv(path)
        else:
            continue
        newtab = pd.concat([df, table], axis = 0).reset_index(drop=True)
    return newtab


def binary_clean(ser):
    sez = []
    for i in range(len(ser)):
        if type(ser[i]) == float:
            sez.append(0)
        elif ser[i] == 'Y' or ser[i] == 'y':
            sez.append(1)
        else:
            sez.append(0)
    return sez

def clean_age(ser):
    sez = []
    for i in range(len(ser)):
        if type(ser[i])== float:
            sez.append(np.nan)
        elif ser[i] == 'M':
            sez.append(1)
        elif ser[i] == 'F':
            sez.append(2)
        else:
            sez.append(3)
    return sez

def clean_post(ser):
    sez = []
    for i in range(len(ser)):
        if type(ser[i])== float:
            sez.append(0)
        else:
            sez.append(1)
    return sez

def clean_contra(ser):
    sez = []
    for i in range(len(ser)):
        if ser[i]== 'None':
            sez.append(0)
        elif type(ser[i])== float:
            sez.append(0)
        else:
            sez.append(1)
    return sez

def clean_arrest(ser):
    #Y' if 'arrest' in x or 'Arrest' in x or 'hold' in x
    sez = []
    for i in range(len(ser)):
        if 'Arrest' in ser[i] or 'arrest' in ser[i] or 'hold' in ser[i]:
            sez.append(1)
        else:
            sez.append(0)
    return sez
    
def clean_time(df):
    df['time_stop'] = pd.to_datetime(df['time_stop'], format= '%H:%M', errors='coerce')
    return df
    

races = {'A':'Middle Eastern or South Asian',
'B':'Black/African American',
'C':'Asian',
'D':'Asian',
'F':'Asian',
'G':'Pacific Islander',
'H':'Hispanic/Latino/a',
'I':'Middle Eastern or South Asian',
'J':'Asian',
'K':'Asian',
'L':'Asian',
'O':'Other',
'P':'Pacific Islander',
'S':'Pacific Islander',
'U':'Pacific Islander',
'V':'Asian',
'W':'White',
'Z':'Middle Eastern or South Asian'}

wanted_columns_pre = ['stop_id', 'stop_cause', 'date_stop', 'time_stop', 'subject_race', 'subject_sex', 'subject_age', 'service_area', 'sd_resident', 'property_seized', 'searched', 'arrested','obtained_consent', 'contraband_found', 'year']
wanted_columns_post = ['stop_id', 'reason_for_stop', 'date_stop', 'time_stop', 'race', 'gend', 'perceived_age', 'beat', 'address_city', 'type_of_property_seized', 'basis_for_search', 'result', 'consented', 'contraband', 'year']


def pre18(df):
    df['subject_race'] = df['subject_race'].map(races)
    df['subject_sex'] = clean_age(df['subject_sex']) 
    df['subject_age'] = pd.to_numeric(df['subject_age'], errors = 'coerce')
    df['service_area'] = pd.to_numeric(df['service_area'], errors = 'coerce')
    df['sd_resident'] = binary_clean(df['sd_resident'])
    df['property_seized'] = binary_clean(df['property_seized'])
    df['searched'] = binary_clean(df['searched'])
    df['arrested'] = binary_clean(df['arrested'])
    df['obtained_consent'] = binary_clean(df['obtained_consent'])
    df['contraband_found'] = binary_clean(df['contraband_found'])
    return df



def post18(df):
    cols = pd.Series(wanted_columns_pre,wanted_columns_post).to_dict()
    df = df.rename(columns = cols)
    df['subject_age'] = pd.to_numeric(df['subject_age'], errors = 'coerce')
    df.sd_resident = df.sd_resident.map({'SAN DIEGO' : 'Y'})
    df.sd_resident = df.sd_resident.map({'Y': 1, np.nan: 0})
    df = df.reset_index(drop=True)
    df['property_seized'] = clean_post(df['property_seized'])
    df['searched'] = clean_post(df['searched'])
    beat_url = 'http://seshat.datasd.org/sde/pd/pd_beats_datasd.geojson'
    pdb = gpd.read_file(beat_url)
    diction = pd.Series(pdb.serv.values,index=pdb.beat).to_dict()
    df['service_area'] = df['service_area'].map(diction)
    df['service_area'] = pd.to_numeric(df['service_area'], errors = 'coerce')
    df['arrested'] = [1 if 'arrest' in i or 'Arrest' in i or 'hold' in i else 0 for i in df.arrested]#clean_arrest(df['arrested'])
    #[1 if 'arrest' in i or 'Arrest' in i or 'hold' in i else 0 for i in df.arrested]
    df['obtained_consent'] = clean_contra(df['obtained_consent'])
    df['contraband_found'] = [0 if 'None' in i or 'none' in i else 1 for i in df.contraband_found]#clean_contra(df['contraband_found'])
    return df[wanted_columns_pre]

def compute_intertwilight(df):
    period = df[(df['time_stop'] >=  pd.to_datetime('17:09', format= '%H:%M')) & (df['time_stop'] <= pd.to_datetime('20:29', format= '%H:%M'))]
    return len(period)/len(df)

    
def why_stop(df):
    return df[(df['stop_cause'] == 'Moving Violation') | (df['stop_cause'] == 'Equipment Violation')] 

def get_VOD_df(df):
    period = df[(df['time_stop'] >=  pd.to_datetime('17:09', format= '%H:%M')) & (df['time_stop'] <= pd.to_datetime('20:29', format= '%H:%M'))]
    #period['time_stop'] = period['time_stop'].apply(lambda x: x.time()) 
    return period
    
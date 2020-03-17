
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
            path = 'data/raw/' + str(i) + '.csv'
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
    sez = []
    for i in range(len(ser)):
        if 'Arrest' in ser[i] or 'arrest' in ser[i] or 'hold' in ser[i]:
            sez.append(1)
        else:
            sez.append(0)
    return sez
    
def clean_time_pre(df):
    df['time_stop'] = pd.to_datetime(df['time_stop'], format= '%H:%M', errors='coerce')
    return df

def clean_time_post(df):
    df['time_stop'] = pd.to_datetime(df['time_stop'], format= '%H:%M:%S', errors='coerce')
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
    df['arrested'] = [1 if 'arrest' in i or 'Arrest' in i or 'hold' in i else 0 for i in df.arrested]
    df['obtained_consent'] = clean_contra(df['obtained_consent'])
    df['contraband_found'] = [0 if 'None' in i or 'none' in i else 1 for i in df.contraband_found]
    return df[wanted_columns_pre]

def compute_intertwilight(df):
    period = df[(df['time_stop'] >=  pd.to_datetime('17:09', format= '%H:%M')) & (df['time_stop'] <= pd.to_datetime('20:29', format= '%H:%M'))]
    return len(period)/len(df)

    
def why_stop(df):
    return df[(df['stop_cause'] == 'Moving Violation') | (df['stop_cause'] == 'Equipment Violation')] 

def get_VOD_df(df):
    period = df[(df['time_stop'] >=  pd.to_datetime('17:09', format= '%H:%M')) & (df['time_stop'] <= pd.to_datetime('20:29', format= '%H:%M'))]
    return period


def has2018(fileslist):
    if '2018.csv' in fileslist:
        return list(fileslist).index('2018.csv')
    else:
        return False
def createtables(lstfile):    
    if has2018(lstfile) == False:
        if len(lstfile) > 1:
            pr2018 = pd.concat([pd.read_csv(f) for f in lstfile]).reset_index(drop = True)

        else:
            pr2018 = pd.read_csv(lstfile[0])
        pr2018['year'] = pr2018['date_stop'].astype(str).str[:4]
        pr2018['year'] = pr2018['year'].astype(int)
        pr2018 = pr2018[wanted_columns_pre]
    else:
        namespre2018 = lstfile[:has2018(lstfile)]
        if len(lstfile) > 1:
            pr2018 = pd.concat([pd.read_csv(f) for f in namespre2018]).reset_index(drop = True)

        else:
            pr2018 = pd.read_csv(lstfile[0])
        pr2018['year'] = pr2018['date_stop'].astype(str).str[:4]
        pr2018['year'] = pr2018['year'].astype(int)
        pr2018 = pr2018[wanted_columns_pre]

        consent = pd.read_csv(lstfile[has2018(lstfile)+1])[['stop_id', 'consented']]
        contra = pd.read_csv(lstfile[has2018(lstfile)+2])[['stop_id', 'contraband']]
        seiz = pd.read_csv(lstfile[has2018(lstfile)+3])[['stop_id', 'type_of_property_seized']]
        race = pd.read_csv(lstfile[has2018(lstfile)+4])[['stop_id', 'race']]
        search = pd.read_csv(lstfile[has2018(lstfile)+5])[['stop_id', 'basis_for_search']]
        whystop = pd.read_csv(lstfile[has2018(lstfile)+6])[['stop_id', 'reason_for_stop']]
        stopresult = pd.read_csv(lstfile[has2018(lstfile)+7])[['stop_id', 'result']]

        pt2018 = pd.read_csv(lstfile[has2018(lstfile)])
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
    return pr2018, pt2018


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

def darkvlight(df, yr):
    vod_df2 = df[df.year==yr]
    light = vod_df2[vod_df2.whatisit=='light']
    light = pd.DataFrame(light.groupby('subject_race')['stop_id'].count() / len(light))
    dark = vod_df2[vod_df2.whatisit=='dark']
    dark = pd.DataFrame(dark.groupby('subject_race')['stop_id'].count() / len(dark))
    conc = pd.concat([light, dark], axis=1)
    return conc, light, dark

def bivar(df, yr):
    ret = get_VOD_df(df)
    ret = ret[ret['year'] == yr]
    ret = ret.sort_values(by = 'time_stop')
    ret['time_stop'] = [str(x.time()) for x in ret.time_stop]
    ret['time_stop'] = [int(x[0:2]) * 3600 + int(x[3:5]) * 60 + int(x[6:]) for x in ret.time_stop]
    forgraph = pd.DataFrame(ret.groupby('time_stop')['stop_id'].count())
    return forgraph

def bivar2(df, yr):
    fif = df[df['year'] == yr]
    fif = fif.sort_values(by = 'time_stop').dropna()
    fif['time_stop'] = [str(x.time()) for x in fif.time_stop]
    fif['time_stop'] = [int(x[0:2]) * 3600 + int(x[3:5]) * 60 + int(x[6:]) for x in fif.time_stop]
    yttt = pd.DataFrame(fif.groupby('time_stop')['stop_id'].count())
    return yttt


    
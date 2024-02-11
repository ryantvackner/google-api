# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 16:48:25 2024

@author: rvackner
"""

# import libraries
import googlemaps
import pyodbc
import pandas as pd
import numpy as np
import json

# google APi key for the account: XXXXX
api_key = 'XXXXX'
gmaps = googlemaps.Client(key=api_key)

# get data from our IVUE database
# looking for active meter status and what service location they are at
# including x, y coords and current emergency address and service address
cnxncis = pyodbc.connect('DSN=XXXXX;PWD=XXXXX')
df_mtr_inv = pd.read_sql_query("SELECT BI_MTR_NBR, BI_MTR_STAT_CD FROM XXXXX.XXXXX WHERE BI_MTR_STAT_CD = 1 or BI_MTR_STAT_CD = 2", cnxncis)
df_mtr_srv = pd.read_sql_query("SELECT BI_SRV_LOC_NBR, BI_MTR_NBR FROM XXXXX.XXXXX", cnxncis)
df_srv_loc = pd.read_sql_query("SELECT BI_SRV_LOC_NBR, BI_X_COORD, BI_Y_COORD, BI_EMER_ADDR, BI_ADDR1 FROM XXXXX.XXXXX", cnxncis)

df_mtr_srv = df_mtr_srv[df_mtr_srv['BI_MTR_NBR'].isin(df_mtr_inv['BI_MTR_NBR'])]
df_srv_loc = df_srv_loc[df_srv_loc['BI_SRV_LOC_NBR'].isin(df_mtr_srv['BI_SRV_LOC_NBR'])]
df_srv_loc['911_Address'] = ""

# create a dict to store the addresses for all the service locations
# iterate through all the service locations
# try to call the API, if it works then save the address, otherwise nan
geocode = {}
for index, srv_loc in df_srv_loc.iterrows():
    srv_loc_nbr = srv_loc["BI_SRV_LOC_NBR"]
    lat = srv_loc["BI_Y_COORD"]
    long = srv_loc["BI_X_COORD"]
   
    try:
        reverse_geocode_result = gmaps.reverse_geocode((lat, long))
        geocode[round(srv_loc_nbr)] = reverse_geocode_result
       
        address = reverse_geocode_result[0]['formatted_address']
    except:
        address = np.nan
   
    df_srv_loc.loc[index, '911_Address'] = address

# save the API results as a JSON file
with open(r'XXXXX\geocode.json', 'w') as f:
    json.dump(geocode, f)


json_address = json.load(open(r'XXXXX\geocode.json'))

df_srv_loc.to_csv(r"XXXXX\911_address.csv", index=False)

# This script is used for populating google sheets with
# anomalies list for user input
# Author: atediarjo@gmail.com

import os
import pygsheets as pg
import pandas as pd

gc = pg.authorize(service_file='key/key.json')
GSHEETS_TARGET = 'odk-form-anomalies' + '-' + os.getenv('PIPELINE_STAGE')

anomalies_list = pd.read_csv('input/anomalies_detection.csv')
ever_resolved = pd.read_csv('input/ever_resolved.csv')

# only output confirmed correct as ever resolved, if manual resolution is done but anomalies is still there, then populate google sheets
ever_resolved = ever_resolved[ever_resolved['resolution_status'] == 'confirmed_correct']

other_status = pd.read_csv('input/resolution_other_status.csv')[['resolution_id', 'resolution_status', 'resolver']]

data_for_spreadsheet = anomalies_list[~anomalies_list['resolution_id'].isin(ever_resolved['resolution_id'].unique())]
data_for_spreadsheet = pd.merge(data_for_spreadsheet, other_status, how='left', on = 'resolution_id')
data_for_spreadsheet['resolution_status'] = data_for_spreadsheet['resolution_status_y'].fillna('to_do')
data_for_spreadsheet.drop(['resolution_status_x', 'resolution_status_y'], inplace=True, axis = 1)
data_for_spreadsheet = data_for_spreadsheet[['resolution_id', 
                                             'KEY',
                                             'form_id',
                                             'anomalies_id',
                                             'anomalies_description',
                                             'anomalies_reports_to_wid',
                                             'resolver',
                                             'resolution_status']].fillna('')


sh = gc.open(GSHEETS_TARGET)
wks = sh.worksheet_by_title('anomalies_resolution_tracker')
wks.clear(start='A1', end = 'Y20000')
wks.set_dataframe(data_for_spreadsheet, (0,0))

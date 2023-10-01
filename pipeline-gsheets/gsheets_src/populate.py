# This script is used for populating google sheets with
# anomalies list for user input
# Author: atediarjo@gmail.com

import pygsheets as pg
import pandas as pd

gc = pg.authorize(service_file='key/key.json')

anomalies_list = pd.read_csv('input/fact_anomalies.csv')
ever_resolved = pd.read_csv('input/ever_resolved.csv')
other_status = pd.read_csv('input/resolution_other_status.csv')[['resolution_id', 'resolution_status']]

data_for_spreadsheet = anomalies_list[~anomalies_list['resolution_id'].isin(ever_resolved['resolution_id'].unique())]
data_for_spreadsheet = pd.merge(data_for_spreadsheet, other_status, how='left', on = 'resolution_id')
data_for_spreadsheet['resolution_status'] = data_for_spreadsheet['resolution_status_y'].fillna('to_do')
data_for_spreadsheet.drop(['resolution_status_x', 'resolution_status_y'], inplace=True, axis = 1)

sh = gc.open('odk_form_anomalies')
wks = sh.worksheet_by_title('anomalies_resolution_tracker')
wks.clear(start='A1', end = 'Y20000')
wks.set_dataframe(data_for_spreadsheet, (0,0))
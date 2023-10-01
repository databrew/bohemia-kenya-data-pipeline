# This script is used updating fact anomalies with resolution tracker
# anomalies list for user input
#
# Author: atediarjo@gmail.com
import os
import pygsheets as pg
import pandas as pd
from datetime import datetime

CURR_TIME = datetime.now()
gc = pg.authorize(service_file='key/key.json')

sh = gc.open('odk_form_anomalies')
wks = sh.worksheet_by_title('anomalies_resolution_tracker')

# get resolution tracker
resolution_tracker_gsheets = pd.DataFrame(wks.get_all_records())
resolution_tracker_gsheets['resolution_id'] = resolution_tracker_gsheets['form_id'] + '__' + resolution_tracker_gsheets['KEY'] + '__' + resolution_tracker_gsheets['anomalies_id']

# remove from sheets
data_to_update_sheet = resolution_tracker_gsheets[~resolution_tracker_gsheets['resolution_status'].isin(['confirmed_correct', 'manual_resolution_done'])]
data_to_remove_upstream = resolution_tracker_gsheets[resolution_tracker_gsheets['resolution_status'].isin(['confirmed_correct', 'manual_resolution_done'])]
data_other_status = resolution_tracker_gsheets[resolution_tracker_gsheets['resolution_status'].isin(['in_progress', 'blocked'])]

# save csv
outname = 'anomalies_resolution.csv'
outdir = './output/anomalies_resolution'
hist_outdir = f'./output/anomalies_resolution_hist/run_time={CURR_TIME}'

if not os.path.exists(outdir):
    os.mkdir(outdir)

if not os.path.exists(hist_outdir):
    os.makedirs(hist_outdir)

fullname = os.path.join(outdir, outname) 
hist_fullname = os.path.join(hist_outdir, outname)       

data_to_remove_upstream.to_csv(fullname,index = False)
data_to_remove_upstream.to_csv(hist_fullname,index = False)


# other status save csv
outname = 'resolution_other_status.csv'
outdir = './input'
if not os.path.exists(outdir):
    os.mkdir(outdir)
fullname = os.path.join(outdir, outname)  
data_other_status.to_csv(fullname,index = False)






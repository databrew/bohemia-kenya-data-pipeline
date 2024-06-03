# This script is used updating ento labs data
# anomalies list for user input
#
# Author: atediarjo@gmail.com
import os
import pygsheets as pg
import pandas as pd
from datetime import datetime


outdir = './ento_labs_output'
if not os.path.exists(outdir):
    os.mkdir(outdir)

CURR_TIME = datetime.now()
GSHEETS_TARGET = 'ento-labs' + '-' + 'production'
gc = pg.authorize(service_file='key/key.json')

sh = gc.open(GSHEETS_TARGET)
worksheets = sh.worksheets()
for worksheet in worksheets:
    if not worksheet.hidden:
        # create filename
        strings = (worksheet.title).split()
        outname = '_'.join([s.lower() for s in strings]) + '.csv'
        fullname = os.path.join(outdir, outname)  
        
        # store dataset
        data = sh.worksheet_by_title(worksheet.title).get_as_df()
        data.to_csv(fullname,index = False)
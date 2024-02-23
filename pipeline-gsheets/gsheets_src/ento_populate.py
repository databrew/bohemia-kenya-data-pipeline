# This script is used for populating google sheets with
# ento labs data
# Author: atediarjo@gmail.com

import os
import pygsheets as pg
import pandas as pd


gc = pg.authorize(service_file='key/key.json')
GSHEETS_TARGET = 'ento-labs' + '-' + os.getenv('PIPELINE_STAGE')

# individual mosquitoes CDC
individual_mosq_cdc = pd.read_csv('input/ento_labs_individual_mosquitoes_cdc.csv', converters = {"Household ID": str} )
individual_mosq_cdc['Household ID'] = individual_mosq_cdc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)

# pooled mosquitoes CDC
pooled_mosq_cdc = pd.read_csv('input/ento_labs_pooled_mosquitoes_cdc.csv', converters = {"Household ID": str} )
pooled_mosq_cdc['Household ID'] = pooled_mosq_cdc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)

# individual mosquitoes CDC
individual_mosq_rc = pd.read_csv('input/ento_labs_individual_mosquitoes_rc.csv', converters = {"Household ID": str} )
individual_mosq_rc['Household ID'] = individual_mosq_rc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)

# pooled mosquitoes CDC
pooled_mosq_rc = pd.read_csv('input/ento_labs_pooled_mosquitoes_rc.csv', converters = {"Household ID": str} )
pooled_mosq_rc['Household ID'] = pooled_mosq_rc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)


#################
# CDC 
#################

indivdual_mosq_cdc_sheet_prep = individual_mosq_cdc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Livestock enclosure ID',
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'Species',
    'physio']].fillna('').sort_values(
        ['Date of collection', 
        'Household ID',
        'Sample tube ID'])

sh = gc.open(GSHEETS_TARGET)
wks = sh.worksheet_by_title('CDC Individual Mosquitoes')

# Fill Date of Collection, Study Arm Houe
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(indivdual_mosq_cdc_sheet_prep, start = (3,1),  copy_head=False)


pooled_mosq_cdc_sheet_prep = pooled_mosq_cdc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Livestock enclosure ID',
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'No. of mosquitoes per tube (Listed Number)',
    'Species']].fillna('').sort_values(
        ['Date of collection', 
        'Household ID',
        'Sample tube ID'])

sh = gc.open(GSHEETS_TARGET)
wks = sh.worksheet_by_title('CDC Pooled Mosquitoes')

# Fill Date of Collection, Study Arm Houe
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(pooled_mosq_cdc_sheet_prep, start = (3,1),  copy_head=False)


#################
# RC
#################

indivdual_mosq_rc_sheet_prep = individual_mosq_rc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'Species Complex',
    'Physiological Status']].fillna('').sort_values(
        ['Date of collection', 
        'Household ID',
        'Sample tube ID'])

sh = gc.open(GSHEETS_TARGET)
wks = sh.worksheet_by_title('RC Individual Mosquitoes')

# Fill Date of Collection, Study Arm Houe
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(indivdual_mosq_rc_sheet_prep, start = (3,1),  copy_head=False)


pooled_mosq_rc_sheet_prep = pooled_mosq_rc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'Number of mosquitos in tube',
    'Species Complex']].fillna('').sort_values(
        ['Date of collection', 
        'Household ID',
        'Sample tube ID'])

sh = gc.open(GSHEETS_TARGET)
wks = sh.worksheet_by_title('RC Pooled Mosquitoes')

# Fill Date of Collection, Study Arm Houe
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(pooled_mosq_rc_sheet_prep, start = (3,1),  copy_head=False)
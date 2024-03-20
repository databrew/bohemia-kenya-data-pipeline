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
individual_mosq_cdc = individual_mosq_cdc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])

# pooled mosquitoes CDC
pooled_mosq_cdc = pd.read_csv('input/ento_labs_pooled_mosquitoes_cdc.csv', converters = {"Household ID": str} )
pooled_mosq_cdc['Household ID'] = pooled_mosq_cdc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
pooled_mosq_cdc = pooled_mosq_cdc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])

# individual mosquitoes RC
individual_mosq_rc = pd.read_csv('input/ento_labs_individual_mosquitoes_rc.csv', converters = {"Household ID": str} )
individual_mosq_rc['Household ID'] = individual_mosq_rc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
individual_mosq_rc = individual_mosq_rc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])

# pooled mosquitoes RC
pooled_mosq_rc = pd.read_csv('input/ento_labs_pooled_mosquitoes_rc.csv', converters = {"Household ID": str} )
pooled_mosq_rc['Household ID'] = pooled_mosq_rc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
pooled_mosq_rc = pooled_mosq_rc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])


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

# CDC individual mosquitoes
wks = sh.worksheet_by_title('CDC Individual Mosquitoes')
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(individual_mosq_cdc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Livestock enclosure ID',
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'Species',
    'physio']], start = (3,1),  copy_head=False)

wks.clear(start='U3', end = 'U20000')
wks.set_dataframe(individual_mosq_cdc[['parity_status']], start = (3,21),  copy_head=False)

# Fill Date of Collection, Study Arm Houe
wks = sh.worksheet_by_title('CDC Pooled Mosquitoes')
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(pooled_mosq_cdc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Livestock enclosure ID',
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'No. of mosquitoes per tube (Listed Number)',
    'Species']], start = (3,1),  copy_head=False)


#################
# RC
#################

# open RC individual mosquitoes
wks = sh.worksheet_by_title('RC Individual Mosquitoes')
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(individual_mosq_rc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'Species Complex',
    'Physiological Status']], start = (3,1), copy_head=False)
wks.clear(start='U3', end = 'X20000')
wks.set_dataframe(individual_mosq_rc[['Oviposited', 'Day oviposited', 'Dead', 'Day Died']], 
                  start = (3,21), copy_head=False)

# open RC pooled mosquitoes
wks = sh.worksheet_by_title('RC Pooled Mosquitoes')
wks.clear(start='A3', end = 'Y20000')
wks.set_dataframe(pooled_mosq_rc[[
    'Date of collection', 
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Box ID',
    'position_in_box',
    'Sample tube ID',
    'Number of mosquitos in tube',
    'Species Complex']], start = (3,1),  copy_head=False)
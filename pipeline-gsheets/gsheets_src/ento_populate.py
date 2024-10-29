# This script is used for populating google sheets with
# ento labs data
# Author: atediarjo@gmail.com

import os
import pygsheets as pg
import pandas as pd


gc = pg.authorize(service_file='key/key.json')
GSHEETS_TARGET = 'ento-labs' + '-' + os.getenv('PIPELINE_STAGE')

def create_list_range(tube_id, num_species):
    output = [f"{tube_id}{o}" for o in list(range(0,num_species))]
    return(output)

# individual mosquitoes CDC
individual_mosq_cdc = pd.read_csv('input/ento_labs_individual_mosquitoes_cdc.csv', converters = {"Household ID": str} )
individual_mosq_cdc['Household ID'] = individual_mosq_cdc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
individual_mosq_cdc = individual_mosq_cdc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])

# pooled mosquitoes CDC
pooled_mosq_cdc = pd.read_csv('input/ento_labs_pooled_mosquitoes_cdc.csv', converters = {"Household ID": str} )
pooled_mosq_cdc['Household ID'] = pooled_mosq_cdc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
pooled_mosq_cdc = pooled_mosq_cdc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])
pooled_mosq_cdc['Sample ID'] = pooled_mosq_cdc.apply(lambda x: create_list_range(
    x['Sample tube ID'],
    x['No. of mosquitoes per tube (Listed Number)']
    ), axis = 1
)
pooled_mosq_cdc = pooled_mosq_cdc.explode('Sample ID').reset_index(drop = True)

# individual mosquitoes RC
individual_mosq_rc = pd.read_csv('input/ento_labs_individual_mosquitoes_rc.csv', converters = {"Household ID": str} )
individual_mosq_rc['Household ID'] = individual_mosq_rc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
individual_mosq_rc = individual_mosq_rc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])

# pooled mosquitoes RC
pooled_mosq_rc = pd.read_csv('input/ento_labs_pooled_mosquitoes_rc.csv', converters = {"Household ID": str} )
pooled_mosq_rc['Household ID'] = pooled_mosq_rc['Household ID'].apply(lambda x: 'HHID: ' + x if x != '' else x)
pooled_mosq_rc = pooled_mosq_rc.fillna('').sort_values(['Date of collection', 'Sample tube ID'])

pooled_mosq_rc['Sample ID'] = pooled_mosq_rc.apply(lambda x: create_list_range(
    x['Sample tube ID'],
    x['Number of mosquitos in tube']
    ), axis = 1
)
pooled_mosq_rc = pooled_mosq_rc.explode('Sample ID').reset_index(drop = True)


##################################
# insecticide resistance
##################################
entolabs_insecticide_resistance = pd.read_csv('input/ento_labs_insecticide_resistance.csv', converters = {"Household ID": str})
entolabs_insecticide_resistance = entolabs_insecticide_resistance.\
    rename(columns = {
        "SubmissionDate": "Date of Collection",
        "test_mosquitoes_age": "Method of Collection",
        "synergist": "Synergist",
        "synergist_assay": "Synergist Assay",
        "insecticide": "Insecticide",	
        "insecticide_concentration": "Insecticide Concentration",
        "replicate": "Replicate",
        "tube_qr": "Tube ID",
        "num_mosquitoes_exposed": "Mosquitoes Exposed",
        "mosq_in_tube": "Number of Mosquitoes per Tube", 
        "mosquito_id": "Mosquito ID"
    }).fillna('').sort_values(['Date of Collection', 'Tube ID','mosq_number'])
entolabs_insecticide_resistance['Dead/Alive'] = entolabs_insecticide_resistance['metric'].apply(lambda x: 'alive' if 'alive' in x else 'dead')

entolabs_insecticide_resistance_cdc_sheet_prep = entolabs_insecticide_resistance[[
        "Date of Collection",
        "Method of Collection",
        "Synergist",
        "Synergist Assay",
        "Insecticide",	
        "Insecticide Concentration",
        "Replicate",
        "Dead/Alive",
        "Tube ID",
        "Mosquitoes Exposed",
        "Number of Mosquitoes per Tube", 
        "Mosquito ID"]]

sh = gc.open('ento-labs-production')

# CDC individual mosquitoes
wks = sh.worksheet_by_title('Insecticide Resistance')
wks.clear(start='A3', end = 'J20000')
wks.set_dataframe(entolabs_insecticide_resistance_cdc_sheet_prep, start = (3,1),  copy_head=False)


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
wks.clear(start='A3', end = 'J20000')
wks.set_dataframe(individual_mosq_cdc[[
    'Date of collection', 
    'Sample tube ID',
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Livestock enclosure ID',
    'Box ID',
    'position_in_box',
    'Species',
    'physio']], start = (3,1),  copy_head=False)

wks.clear(start='U3', end = 'U20000')
wks.set_dataframe(individual_mosq_cdc[['parity_status']], start = (3,21),  copy_head=False)

# Fill Date of Collection, Study Arm Houe
wks = sh.worksheet_by_title('CDC Pooled Mosquitoes')
wks.clear(start='A3', end = 'K20000')
wks.set_dataframe(pooled_mosq_cdc[[
    'Date of collection', 
    'Sample tube ID',
    'Sample ID',
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Livestock enclosure ID',
    'Box ID',
    'position_in_box',
    'No. of mosquitoes per tube (Listed Number)',
    'Species',
    ]], start = (3,1),  copy_head=False)


#################
# RC
#################

# open RC individual mosquitoes
wks = sh.worksheet_by_title('RC Individual Mosquitoes')
wks.clear(start='A3', end = 'I20000')
wks.set_dataframe(individual_mosq_rc[[
    'Date of collection', 
    'Sample tube ID',
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Box ID',
    'position_in_box',
    'Species Complex',
    'Physiological Status']], start = (3,1), copy_head=False)
wks.clear(start='U3', end = 'X20000')
wks.set_dataframe(individual_mosq_rc[['Oviposited', 'Day oviposited', 'Dead', 'Day Died']], 
                  start = (3,21), copy_head=False)

# open RC pooled mosquitoes
wks = sh.worksheet_by_title('RC Pooled Mosquitoes')
wks.clear(start='A3', end = 'J20000')
wks.set_dataframe(pooled_mosq_rc[[
    'Date of collection', 
    'Sample tube ID',
    'Sample ID',
    'Cluster', 
    'Arm', 
    'Household ID', 
    'Box ID',
    'position_in_box',
    'Number of mosquitos in tube',
    'Species Complex']], start = (3,1),  copy_head=False)
import requests
import json
import pandas as pd
import os
import shutil
import logging

# set session and configuraion
shutil.rmtree('metadata')
os.mkdir('metadata')
logging.basicConfig(filename='run.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
session = requests.Session()
session.auth = (os.getenv('ODK_USERNAME'), os.getenv('ODK_PASSWORD'))

# Global Variables
SOURCE_PROJECT_ID = 19
TARGET_PROJECT_ID = 27

# this function is used to create a form based on an xml file
def create_form(target_project_id, form_id, xml_form_definition):
  logging.info(f'parsing {form_id} xml to {target_project_id}')
  try:
    headers = {
      'Content-Type': 'application/xml'
    }
    r = session.post(
      f'https://databrew.org/v1/projects/{target_project_id}/forms?ignoreWarnings=false&publish=false',
      data = xml_form_definition,
      headers = headers
    )
    logging.info(f'parsing {form_id} xml to {target_project_id} successful')
    return(r.content)
  except requests.exceptions.HTTPError as e:
    logging.error(e.response.text)
      
# this function is used to get file attachment
def upload_attachment(target_project_id, form_id, filename, binary_values):
  logging.info(f'parsing {form_id} with attachment {filename}')
  filename = os.path.basename(filename)
  try:
    headers = {
      'Content-Type': 'application/xml'
    }
    r = session.post(
      f'https://databrew.org/v1/projects/{target_project_id}/forms/{form_id}/draft/attachments/{filename}',
      data = binary_values,
      headers = headers
    )
    logging.info(f'parsing {form_id} with attachment {filename} successful')
    return(r.content)
  except requests.exceptions.HTTPError as e:
    logging.error(e.response.text)

# this function is used to publish draft
def publish_draft(target_project_id, form_id, version):
  logging.info(f'publishing {form_id} draft to {target_project_id}')
  try:
    headers = {
      'Content-Type': 'application/xml'
    }
    r = session.post(
      f'https://databrew.org/v1/projects/{target_project_id}/forms/{form_id}/draft/publish?version={version}'
    )
    logging.info(f'publishing {form_id} draft to {target_project_id} successful')
    return(r.content)
  except requests.exceptions.HTTPError as e:
    logging.error(e.response.text)

########################################################
# 1. Get Forms and their attachments
########################################################

response = session.get(f'https://databrew.org/v1/projects/{SOURCE_PROJECT_ID}/forms')
response_df = pd.DataFrame(json.loads(response.content))

response_df['form_definition'] = (response_df['xmlFormId'].apply(
    lambda form_id: session.get(f'https://databrew.org/v1/projects/{SOURCE_PROJECT_ID}/forms/{form_id}.xml').content))
response_df['form_attachment'] = (response_df['xmlFormId'].apply(
    lambda form_id: json.loads(session.get(f'https://databrew.org/v1/projects/{SOURCE_PROJECT_ID}/forms/{form_id}/attachments').content)))

# normalize attachment if multiple
response_df_norm = response_df.explode('form_attachment')

########################################################
# 2. Create mapping files
########################################################

mapping = {}
mapping['source_project_id'] = []
mapping['form_id'] = []
mapping['form_definition'] = []
mapping['form_attachment_filepath'] = []
mapping['form_attachment_binaries'] = []

for index, row in response_df_norm.iterrows():
    source_project_id = row['projectId']
    form_id = row['xmlFormId']
    form_definition = row['form_definition']
    form_attachment = row['form_attachment']

    mapping['source_project_id'].append(source_project_id)
    mapping['form_id'].append(form_id)
    mapping['form_definition'].append(form_definition)

    try:
        form_attachment_name = form_attachment['name']
        response = session.get(
            f'https://databrew.org/v1/projects/{source_project_id}/forms/{form_id}/attachments/{form_attachment_name}')
        
        target_path = os.path.join('metadata', form_attachment_name)
        mapping['form_attachment_binaries'].append(response.content)
        open(target_path, "wb").write(response.content)
        mapping['form_attachment_filepath'].append(target_path)
    except TypeError:
        mapping['form_attachment_filepath'].append(pd.NA)
        mapping['form_attachment_binaries'].append(pd.NA)
mapping = pd.DataFrame(mapping)
mapping['target_project_id'] = TARGET_PROJECT_ID


########################################################
# 3. Create forms, attach metadata and publish draft forms
########################################################

# create forms
mapping.apply(lambda row: create_form(
    target_project_id = TARGET_PROJECT_ID, 
    form_id = row['form_id'],
    xml_form_definition = row['form_definition']), axis = 1)

# upload form attachments
mapping.dropna(subset = ['form_attachment_filepath']).apply(
    lambda row: upload_attachment(
        target_project_id = TARGET_PROJECT_ID, 
        form_id = row['form_id'], 
        binary_values = row['form_attachment_binaries'],
        filename = row['form_attachment_filepath']), axis = 1)

# publish draft
mapping.apply(lambda row: publish_draft(
    target_project_id = TARGET_PROJECT_ID, 
    form_id = row['form_id'],
    version = 'TESTING'), 
    axis = 1)
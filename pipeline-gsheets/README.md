## Pipeline Google Sheets

### Workflow

![workflow](https://lucid.app/publicSegments/view/c5b95930-5292-4c91-95b8-12e9a3d69c5c/image.jpeg)


### Data Custodian Guideline 

1. Go to [odk_form_anomalies](https://docs.google.com/spreadsheets/d/1i98uVuSj3qETbrH7beC8BkFmKV80rcImGobBvUGuqbU) spreadsheet

2. The first page will be the `anomalies_resolution_tracker` which will be used to track changes being done to anomalies

3. In the `resolution_status` column - data custodians can perform `confirmed_correct`, meaning that no resolution is required.

4. If a resolution is required on a particular ID, data custodian can take the action by filling out `manual_resolution_by_id` sheet. If resolution is specific to an anomalies being detected, please specify `resolution_id` in the same sheet for tracking purposes

5. If a wider scope resolution is required (>10 IDs require the same resolution method) - please report to `#data-ops` slack channel for an automated approach from DataBrew side

6. Once resolution has been made made, please update `anomalies_resolution_tracker` to commit your changes 

(Make sure to add in your name in the resolver field for tracking purposes)

7. Anomalies resolution tracker sheet will be updated **12:00 AM EAT** everyday
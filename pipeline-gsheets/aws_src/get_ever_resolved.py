import os
import boto3
import awswrangler as aw
import pandas as pd

bucket_name = os.getenv('BUCKET_NAME')
data = aw.s3.read_csv(f's3://{bucket_name}/bohemia_prod/anomalies_resolution_hist/', dataset = True)
data = data.drop_duplicates(subset=['resolution_id'])
data.to_csv('input/ever_resolved.csv', index = False)

aw.s3.to_csv(
    df = data,
    path = f's3://{bucket_name}/bohemia_prod/anomalies_resolution/anomalies_resolution.csv'
)

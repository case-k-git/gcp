from google.cloud import bigquery
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


# Get From GBQ
sql = """
  SELECT
    pickup_datetime, pickup_longitude, pickup_latitude, dropoff_longitude,
    dropoff_latitude, passenger_count, trip_distance, tolls_amount, 
    fare_amount, total_amount 
  FROM `nyc-tlc.yellow.trips`
  LIMIT 10
"""

bqclient = bigquery.Client()
trips = bqclient.query(sql).to_dataframe()
trips.to_csv('./trips_local.csv',index=None)

# Upload To GCS
bucket_name = ""
project_name = ""
gcs_path = './trips_gcs.csv'
local_path = './trips_local.csv'
client = gcs.Client(project_name)
bucket = client.get_bucket(bucket_name)

blob = bucket.blob(gcs_path)
blob.upload_from_filename(filename=local_path)

# Read From GCS
blob = gcs.Blob(gcs_path , bucket)
content = blob.download_as_string()
train = pd.read_csv(BytesIO(content))
print(train.head(1))

# Load  From Gcs To GBQ
dataset_ref = bqclient.dataset(dataset_id='tst')
table_ref = dataset_ref.table(table_id='trips')
job_config = bigquery.LoadJobConfig()
job_config.source_format = 'CSV'
#job_config.skip_leading_rows = 1
job_config.autodetect = True

#from StringIO import StringIO
load_job = bqclient.load_table_from_file(
    file_obj=BytesIO(content) ,
    destination=table_ref, 
    job_config=job_config
)
load_job.result()
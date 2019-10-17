def if_tbl_exists(client, table_ref):
    from google.cloud.exceptions import NotFound
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False

def load_to_gbq(data, context):
    from google.cloud import bigquery
    import pandas as pd
    from google.cloud.exceptions import NotFound
    
    # read from gcs
    bucket_name = data['bucket']
    file_name = data['name']
    project_id = "[project id ]"
    
    # create data set
    dataset_name = 'my_dataset'
    bq_client = bigquery.Client(project=project_id)
    dataset = bigquery.Dataset(bq_client.dataset(dataset_name ))
    
    # table name
    table_ref = dataset.table(file_name[0:-4])    
    # table check 
    tbl_exists = if_tbl_exists(bq_client, table_ref)
    print(tbl_exists)
    if tbl_exists == False:
        """
        SCHEMA = [
            bigquery.SchemaField('col1', 'STRING', mode='required'),
            bigquery.SchemaField('col2', 'INTEGER', mode='required'),
        ]
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = bq_client.create_table(table)      # API request
        """
        GS_URL = 'gs://{}/{}'.format(bucket_name, file_name)
        job_id_prefix = "my_job"
        job_config = bigquery.LoadJobConfig()
        #job_config.skip_leading_rows = 1
        job_config.source_format = 'CSV'
        job_config.autodetect = True
        load_job = bq_client.load_table_from_uri(
            GS_URL, table_ref, job_config=job_config,
            job_id_prefix=job_id_prefix)  # API request
        load_job.result()  # Waits for table load to complete.
        print('load finish')
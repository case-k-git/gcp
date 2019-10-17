def _dataflow_job_start(data, context):
    # read from gcs
    from googleapiclient.discovery import build
    PROJECTID = data['bucket']
    file_name = data['name']

    job = 'job from cloud functions_3'
    #template = "gs://{}/template/custom_template_1008".format(PROJECTID)
    template = "gs://{}/template/GCS_TO_GCS_2".format(PROJECTID)
    parameters = {
         #'input': "gs://{}/{}".format(PROJECTID,file_name),
         #'output': "gs://{}/output/{}".format(PROJECTID,file_name),
         'inputFile': "gs://{}/{}".format(PROJECTID,file_name),
         'outputFile': "gs://{}/output/{}".format(PROJECTID,file_name),
     }

    
    service  = build("dataflow","v1b3",cache_discovery=False)
    #templates = service.projects().templates()
        
    request = service.projects().templates().launch(
        projectId=PROJECTID,
        gcsPath=template,
        body={
            'jobName': job,
            'parameters': parameters,
        }
    )
    return request.execute()
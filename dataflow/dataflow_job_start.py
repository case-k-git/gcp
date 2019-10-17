def _dataflow_job_start():
    # 必要なライブラリ
    from oauth2client.client import GoogleCredentials
    from oauth2client.service_account import ServiceAccountCredentials
    #from apiclient.discovery import build
    from googleapiclient.discovery import build
    
    # クライアントライブラリを初期化
    credentials = GoogleCredentials.get_application_default()
    service  = build("dataflow","v1b3",credentials=credentials)
    templates = service.projects().templates()
    
    # プロジェクトID
    PROJECTID = 'project id '
    
    # JOBの設定項目
    BODY = {
        "jobName": "job",
        "gcsPath": "gs://{}/template/custom_template_1008".format(PROJECTID),
        "environment":{
            "tempLocation": "gs://{}/temp".format(PROJECTID)
        },
        "parameters": {
            "input": "gs://{}/sample2.csv".format(PROJECTID),
            "output": "gs://{}/output/sample2_2.csv".format(PROJECTID),
        },
    }
    
    # JOBの実行
    dfrequest = service.projects().templates().create(projectId=PROJECTID, body=BODY)
    return dfrequest.execute()
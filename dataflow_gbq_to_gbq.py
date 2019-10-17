#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# -*- coding: utf-8 -*-
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# プロジェクトID
PROJECTID = ''

# オプション設定
class MyOptions(PipelineOptions):
    PROJECTID = ''
    @classmethod  
    def _add_argparse_args(cls, parser):        
        # 実行時に指定するパラメータ
        parser.add_value_provider_argument('--inputFile',
                            help='InputFile for the pipeline',
                            default='gs://{}/output/sample2.csv'.format(PROJECTID))
        parser.add_value_provider_argument('--outputFile',
                            help='OutputFile for the pipeline',
                            default='gs://{}/output/sample2.csv'.format(PROJECTID))
            
# オプション設定
myoptions = MyOptions()
options = beam.options.pipeline_options.PipelineOptions(options=myoptions)

# GCP関連オプション
gcloud_options = options.view_as(
  beam.options.pipeline_options.GoogleCloudOptions)
gcloud_options.project = PROJECTID
gcloud_options.job_name = 'jobgbqtogbq'
gcloud_options.staging_location = 'gs://{}/staging'.format(PROJECTID)
gcloud_options.temp_location = 'gs://{}/tem'.format(PROJECTID)

# テンプレート配置
#gcloud_options.template_location = 'gs://{}/template/GBQ_TO_GBQ'.format(PROJECTID)

# 標準オプション（実行環境を設定）
std_options = options.view_as(
  beam.options.pipeline_options.StandardOptions)
std_options.runner = 'DataflowRunner'

table_spec = 'dataset_tst.table_gbq_to_gbq'
table_schema= 'word:STRING,word_count:INTEGER'

p = beam.Pipeline(options=options)
# p | 'read from gcs' >> beam.io.ReadFromText(myoptions.inputFile) 
query = "SELECT word , word_count " "FROM `bigquery-public-data.samples.shakespeare`" "LIMIT 10"

(p | 'read' >> beam.io.Read(beam.io.BigQuerySource(project=PROJECTID,
                                                   use_standard_sql=True,
                                                   query=query))
  | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{}:{}'.format(PROJECTID,table_spec), schema=table_schema)
 )
p.run()


# In[ ]:





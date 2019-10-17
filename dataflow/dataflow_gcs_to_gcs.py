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
        #parser.add_argument('--input',
        #                    help='Input for the pipeline',
        #                    default='gs://{}/sample2.csv'.format(PROJECTID))
        # parser.add_argument('--output',
        #                    help='Output for the pipeline',
        #                    default='gs://{}/output/sample2.csv'.format(PROJECTID))
        
        # 実行時に指定するパラメータ
        parser.add_value_provider_argument('--inputFile',
                            help='InputFile for the pipeline',
                            default='gs://{}/output/sample1016.csv'.format(PROJECTID))
        parser.add_value_provider_argument('--outputFile',
                            help='OutputFile for the pipeline',
                            default='gs://{}/output/sample1006.csv'.format(PROJECTID))
            
# オプション設定
myoptions = MyOptions()
options = beam.options.pipeline_options.PipelineOptions(options=myoptions)

# GCP関連オプション
gcloud_options = options.view_as(
  beam.options.pipeline_options.GoogleCloudOptions)
gcloud_options.project = PROJECTID
gcloud_options.job_name = 'job1016'
gcloud_options.staging_location = 'gs://{}/staging'.format(PROJECTID)
gcloud_options.temp_location = 'gs://{}/tem'.format(PROJECTID)

# テンプレート配置
#gcloud_options.template_location = 'gs://{}/template/{}/-twproc_tmp'.format(PROJECTID,PROJECTID)
#gcloud_options.template_location = 'gs888/{}/template/-twproc_tmp2'.format(PROJECTID)
gcloud_options.template_location = 'gs://{}/template/GCS_TO_GCS_5'.format(PROJECTID)

# 標準オプション（実行環境を設定）
std_options = options.view_as(
  beam.options.pipeline_options.StandardOptions)
std_options.runner = 'DataflowRunner'

p = beam.Pipeline(options=options)
( p | 'read' >> beam.io.ReadFromText(myoptions.inputFile) 
   | 'write' >> beam.io.WriteToText(myoptions.outputFile,num_shards=1,shard_name_template=''))
p.run()


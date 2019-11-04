#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install google-cloud-pubsub')
get_ipython().system('pip install pandas')
# ! gcloud auth login
# ! gcloud auth application-default login


# ### Python Client for Google Cloud Pub / Sub
# https://googleapis.dev/python/pubsub/latest/index.html

# In[1]:


from google.cloud import pubsub
import pandas as pd
from google.cloud import pubsub_v1


# ### Set options

# In[26]:


#!gcloud pubsub topics create my-topic3
#!gcloud pubsub subscriptions create my-sub3 --topic my-topic2


# In[38]:


topic_name = 'my-topic'
subscription_name = 'my-subscription'
input_path = './sensor_obs2008.csv.gz'
project_id = ''


# ###  Read Data

# In[36]:


get_ipython().system('gsutil cp gs://cloud-training-demos/sandiego/sensor_obs2008.csv.gz .')
data  =  pd.read_csv(input_path)
data.head()


# ### Publish message

# In[19]:


publisher = pubsub_v1.PublisherClient()
#topic_path = publisher.topic_path(project_id, topic_name)
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=project_id,
    topic=topic_name,  # Set this to something appropriate.
)
publisher.create_topic(topic_name)


# In[34]:


for i , v in data.head(10).iterrows():
    message = str(list(v.values)).encode('utf-8')
    future = publisher.publish(topic_name, data=message)
    print(message)


# ### Subscribe message

# In[28]:


subscriber = pubsub_v1.SubscriberClient()
subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=project_id,
    sub=subscription_name ,  # Set this to something appropriate.
)
subscriber.create_subscription(
    name=subscription_name, topic=topic_name)


# In[35]:


# ここを実行するとメッセージが確認できなくなる、
def callback(message):
    print(message.data)
    message.ack()
future = subscriber.subscribe(subscription_name, callback)


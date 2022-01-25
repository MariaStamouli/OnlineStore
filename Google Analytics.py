#!/usr/bin/env python
# coding: utf-8

# In[1]:


# enable BiqQeury API
import pandas as pd
from google.cloud import bigquery
#create json file
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'leafy-habitat-233611-0c4256566ed5.json'
client = bigquery.Client()


# # Story 1: E-Commerce Conversion Rate

# ### As a Product owner I need to know the ecommerce Conversion rate for the day X. I’d like to be able to breakdown the above over up to two dimensions : 
# 
#  **- UserType (whether the session refers to a New or Returning user)**<br><br>
#  **- Platform (whether the session occurred into desktop; ie Web or Mobile)**

# ### Conversion Rate Definition 

# Generally, conversion rate is a metric that shows that a visitor of a website provokes an action. An action can be a purchase or just a newsletter registration. Conversion rate is computed as the number of (e.g. purchases) divided by the total number of sessions (in our dataset every row represents a session).
# 
# Regarding our dataset, we can use specific fields to compute the conversion rate.
# One field is the **"hits.transaction.transactionId"**. In case this field is not null, it means that a transaction is happening.
# Another field is the **"hits.eCommerceAction.action_type"**. 
# When this field has the value '6' it means that a user has completed to a purchase.

# In[2]:


def categorise(row):  
    if row['visitNumber'] == 1:
        return 'New'
    else:
        return 'Returning'


# In[3]:


def categorise_action(row):  
    if row['eCommerceAction.action_type'] == '1':
        return 'Click through of product lists'
    elif row['eCommerceAction.action_type'] == '2':
        return 'Product detail views'
    elif row['eCommerceAction.action_type'] == '3':
        return 'Add product(s) to cart'
    elif row['eCommerceAction.action_type'] == '4':
        return 'Remove product(s) from cart'
    elif row['eCommerceAction.action_type'] == '5':
        return 'Check out'
    elif row['eCommerceAction.action_type'] == '6':
        return 'Completed purchase'
    elif row['eCommerceAction.action_type'] == '7':
        return 'Refund of purchase'
    elif row['eCommerceAction.action_type'] == '8':
        return 'Checkout options'
    else:
        return 'Unknown'


# In[4]:


def conv_rate(df):
    
    conv_rate_new = df[df['User']=='New']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['User']=='New'])

    conv_rate_ret = df[df['User']=='Returning']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['User']=='Returning'])
    
    conversion_rate = pd.DataFrame([[conv_rate_new[0],conv_rate_ret[0]]],columns=['ConversionNew','ConversionReturning'])
    
    return conversion_rate


# In[5]:


def conv_rate_dvc(df):
    
    conv_rate_desk = df[df['deviceCategory']=='desktop']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['deviceCategory']=='desktop'])
    conv_rate_mob = df[df['deviceCategory']=='mobile']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['deviceCategory']=='mobile'])
    conversion_tab = df[df['deviceCategory']=='tablet']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['deviceCategory']=='tablet'])
    conversion_rate = pd.DataFrame([[conv_rate_desk[0],conv_rate_mob[0],conversion_tab[0]]],columns=['ConversionDesktop','ConversionMobile','ConversionTablet'])    
    
    return conversion_rate


# In[6]:


def conv_rate_oper(df):
    
    conv_rate_and = df[df['operatingSystem']=='Android']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['operatingSystem']=='Android'])
    conv_rate_ios = df[df['operatingSystem']=='iOS']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['operatingSystem']=='iOS'])
    conversion_chrome = df[df['operatingSystem']=='Chrome OS']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['operatingSystem']=='Chrome OS'])
    conv_rate_lin = df[df['operatingSystem']=='Linux']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['operatingSystem']=='Linux'])
    conv_rate_mac = df[df['operatingSystem']=='Macintosh']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['operatingSystem']=='Macintosh'])
    conversion_win = df[df['operatingSystem']=='Windows']['eCommerceAction.action_type'].value_counts().to_frame().loc['6']/len(df[df['operatingSystem']=='Windows'])
    
    
    conversion_rate = pd.DataFrame([[conv_rate_and[0],                                     conv_rate_ios[0],                                     conversion_chrome[0],                                     conv_rate_lin[0],                                      conv_rate_mac[0],                                      conversion_win[0]                                     ]],columns=['ConversionAndroid','ConversioniOS','ConversionChrome',                                                 'ConversionLinux','ConversionMacintosh','ConversionWindows']
                                    )    
    
    return conversion_rate


# In[7]:


# Conversion Rate based on user category
def query_users(start_date,end_date): 
   
    my_query = """
    
      SELECT  date,
              visitNumber,
              h.eCommerceAction.action_type
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, UNNEST(hits) as h
       WHERE _table_suffix BETWEEN @start_date
                               AND @end_date
         AND  h.eCommerceAction.action_type<>'0'
       
    """
    job_config = bigquery.QueryJobConfig(
    query_parameters = [
         bigquery.ScalarQueryParameter('start_date', 'STRING', start_date),
         bigquery.ScalarQueryParameter('end_date', 'STRING', end_date),
       ]
   )
    
    query_job = client.query(my_query, job_config=job_config)  
    
    results = query_job.result()  
    
    #for row in results:
    #    print(row)
    
    l=[]
    for row in results:
        l.append([row.date, row.visitNumber,row.action_type])
    pd_visits = pd.DataFrame(l,columns=['date','visitNumber','eCommerceAction.action_type'])
    pd_visits['User'] = pd_visits.apply(lambda row: categorise(row), axis=1)
    pd_visits['ActionDesc'] = pd_visits.apply(lambda row: categorise_action(row), axis=1)  
    conversion = conv_rate(pd_visits)
    return conversion


# In[8]:


# Conversion Rate based on device category
def query_devices(start_date,end_date): 
   
    my_query = """
    
      SELECT  date,
              device.deviceCategory,
              h.eCommerceAction.action_type
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, UNNEST(hits) as h
       WHERE _table_suffix BETWEEN @start_date
                               AND @end_date
         AND  h.eCommerceAction.action_type<>'0'
       
    """   
    
    job_config = bigquery.QueryJobConfig(
    query_parameters = [
         bigquery.ScalarQueryParameter('start_date', 'STRING', start_date),
         bigquery.ScalarQueryParameter('end_date', 'STRING', end_date),
       ]
   )
    
    query_job = client.query(my_query, job_config=job_config)  
    
    results = query_job.result()  
    
    #for row in results:
    #    print(row)
    
    l=[]
    for row in results:
        l.append([row.date, row.deviceCategory,row.action_type])
    pd_devices = pd.DataFrame(l,columns=['date','deviceCategory','eCommerceAction.action_type'])
    pd_devices['ActionDesc'] = pd_devices.apply(lambda row: categorise_action(row), axis=1)
    
    conversion = conv_rate_dvc(pd_devices)
    return conversion


# In[9]:


# Conversion Rate based on platform category
def query_platform(start_date,end_date): 
   
    my_query = """
    
      SELECT  date,
              device.operatingSystem,
              device.isMobile,
              h.eCommerceAction.action_type
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, UNNEST(hits) as h
       WHERE _table_suffix BETWEEN @start_date
                               AND @end_date
         AND  h.eCommerceAction.action_type<>'0'
       
    """   
    
    job_config = bigquery.QueryJobConfig(
    query_parameters = [
         bigquery.ScalarQueryParameter('start_date', 'STRING', start_date),
         bigquery.ScalarQueryParameter('end_date', 'STRING', end_date),
       ]
   )
    
    query_job = client.query(my_query, job_config=job_config)  
    
    results = query_job.result()  
    
    #for row in results:
    #    print(row)
    
    l=[]
    for row in results:
        l.append([row.date, row.operatingSystem,row.isMobile,row.action_type])
    pd_platform = pd.DataFrame(l,columns=['date','operatingSystem','isMobile','eCommerceAction.action_type'])
    pd_platform['ActionDesc'] = pd_platform.apply(lambda row: categorise_action(row), axis=1)
    
    conversion = conv_rate_oper(pd_platform)
    return conversion, pd_platform


# In[10]:


#Test for specific days (User category)
try:
    print(query_users('20160801','20160801'))
except:
    print('No purchases for specific category of user and specific day. Try to change day or extend the period') 


# In[11]:


#Test for specific days (Device category)
try:
    print(query_devices('20160801','20160801'))
except:
    print('No purchases for specific device and day. Try to change day or extend the period') 


# In[12]:


#Test for specific days (Operating System)
try:
    print(query_platform('20160801','20161001')[0])
except:
    print('No purchases for specific device and day. Try to change day or extend the period') 


# In[ ]:





# # Story 2: Customer Time to convert

# ### As a Product owner I’d like a list of all users with the timestamp of their first session and their time to convert

# -The first timestamp of a user is the field **visitStartTime** from the dataset.<br><br>
# -The time to convert is given by the field **time**.

# In[13]:


#import datetime
#def convert_from_ms( milliseconds ): 
#    
#    """
#    This function converts milliseconds to string with hour,minute, second and returns a datetime timestamp
#        
#    """
#    
#    seconds, milliseconds = divmod(milliseconds,1000) 
#    minutes, seconds = divmod(seconds, 60) 
#    hours, minutes = divmod(minutes, 60) 
#    days, hours = divmod(hours, 24) 
#    seconds = seconds + milliseconds/1000 
#    seconds = int(seconds) #round to the below integer#
#    
#    timestring  = str(hours)+':'+str(minutes)+':'+str(seconds)
#    
#    result = datetime.datetime.strptime(timestring, '%H:%M:%S')
#    
#    return result


# In[14]:


# First timestamp of a user & conversion time
def query_conv_time(): 
    
    
    table_id = "leafy-habitat-233611.VisitorsConversion.VisitorsConv"

    schema = [
        bigquery.SchemaField("fullVisitorId", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("visitStartTime", "TIMESTAMP", mode="REQUIRED"),
         bigquery.SchemaField("timeConvert", "TIMESTAMP", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    
    job_config = bigquery.QueryJobConfig(destination=table_id)
    
    
    my_query = """
        
    SELECT 
            fullVisitorId,
            --min(visitStartTime) AS visitStartTime,
            --max(h.time) AS timeConvert,
            TIMESTAMP_SECONDS(min(visitStartTime)) AS visitStartTime,
            TIMESTAMP_MILLIS(max(h.time)) as timeConvert
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`, UNNEST(hits) as h
    WHERE _table_suffix BETWEEN '20160801'
                            AND '20170801'
    AND visitNumber = 1                             
    AND h.eCommerceAction.action_type='6'
    GROUP BY fullVisitorId
    
        """
    
    query_job = client.query(my_query, job_config=job_config)  # Make an API request.
    results = query_job.result()  # Wait for the job to complete.

    print("Query results loaded to the table {}".format(table_id))
 
    l=[]
    for row in results:
        l.append([row.fullVisitorId,                  row.visitStartTime,                  row.timeConvert])
    df_time = pd.DataFrame(l,columns=['fullVisitorId',                                      'visitStartTime',                                      'timeConvert'
                                      ])
    #df_time['dt_visitStartTime'] = [datetime.datetime.fromtimestamp(x) for x in df_time['visitStartTime']]
    #df_time['dt_timeConvert'] = df_time['timeConvert'].apply(convert_from_ms)

    return df_time 


# In[15]:


query_conv_time()


# In[ ]:





# In[ ]:





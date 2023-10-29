#!/usr/bin/env python
# coding: utf-8

# In[2]:


import boto3


# In[3]:


import pandas as pd
import psycopg2
import json


# In[4]:


ec2 = boto3.resource('ec2',
                   region_name='us-east-1',
                   aws_access_key_id,
                   aws_secret_access_key
                   )


# In[5]:


s3 = boto3.resource('s3',
                     region_name='us-east-1',
                     aws_access_key_id
                     aws_secret_access_key
                     )


# In[6]:


iam = boto3.client('iam',
                   region_name='us-east-1',
                   aws_access_key_id,
                   aws_secret_access_key
                    )


# In[7]:


redshift = boto3.client('redshift',
                       region_name='us-east-1',
                       aws_access_key_id,
                       aws_secret_access_key


# In[8]:


bucket=s3.Bucket('dehou-bucket')


# In[9]:


log_data_files = [filename.key for filename in bucket.objects.filter(Prefix='')]
log_data_files


# In[10]:


roleArn = iam.get_role(RoleName='redshift-s3-Access')['Role']['Arn']


# In[12]:


import redshift_connector


# In[13]:


try:
    conn = psycopg2.connect(host="my-first-workgroup.us-east-1.redshift-serverless.amazonaws.com", dbname="dev", port=5439)
except psycopg2.Error as e:
    print("Error")
    print(e)
    
conn.set_session(autocommit=True)


# In[15]:


try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print('error')
    print(e)


# In[16]:


print(cur)


# In[ ]:


try:
    cur.execute("""create table users (
    userid integer not null distkey,
    username char(8),
    firstname varchar(30),
    lastname varchar(30),
    city varchar(30),
    state char(2),
    email varchar(100),
    phone char(14),
    likesports boolean,
    liketheatre boolean,
    likeconcerts boolean,
    likejazz boolean,
    likeclassical boolean,
    likeopera boolean,
    likerock boolean,
    likevegas boolean,
    likebroadway boolean,
    likemusicals boolean
    );""")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)


# In[ ]:


try:
    cur.execute(""" create table venue(
    venueid smallint not null distkey,
    venuename varchar(100),
    venuecity varchar(30),
    venuestate char(2),
    venueseats integer
     );  
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)


# In[ ]:


try:
    cur.execute("""create table catgory(
    catid smallint not null distkey,
    catgroup varchar(10),
    catname varchar(10),
    catdesc varchar(50));
    
    
    create table date(
    dateid smallint not null distkey,
    caldate date not null,
    day character(3) not null,
    week smallint not null,
    month character(5) not null,
    qtr character(5) not null,
    year smallint not null,
    holiday boolean default('N')
    );
    
    create table event(
    evenid integer not null distkey,
    venueid smallint not null,
    catid smallint not null,
    dateid smallint not null sortkey,
    eventname varchar(200),
    starttime timestamp
    );
    
    
    create table listing(
    listid integer not null distkey,
    sellerid integer not null,
    eventid integer not null,
    dateid smallint not null sortkey,
    numtickets smallint not null,
    priceperticket decimal(8,2),
    totalprice decimal(8,2),
    listtime timestamp
    );    
    """)
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[ ]:


try:
    cur.execute("""
    copy users from 's3://dehou-bucket/allusers_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam:::role/redshift-s3-Access'
    delimiter '|'
    region 'us-east-1'
    """)
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[ ]:


try:
    cur.execute("""
    select * from users;
    """)
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[ ]:


row = cur.fetchone()
print(row)


# In[ ]:


try: 
    cur.execute("""
    copy catgory from 's3://dehou-bucket/category_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam:::role/redshift-s3-Access'
    delimiter '|'
    region 'us-east-1'
    """)
except psycopg2.Error as e :
    print("Error: issue creating table ")
    print(e)


# In[ ]:


try: 
    cur.execute("""
    copy venue from 's3://dehou-bucket/venue_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam:::role/redshift-s3-Access'
    delimiter '|'
    region 'us-east-1'

    """)
except psycopg2.Error as e:
    print('Erro: issue creating table')
    print(e)


# In[ ]:


try:
    cur.execute("""
    copy date from 's3://dehou-bucket/date2008_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam:::role/redshift-s3-Access'
    delimiter '|'
    region 'us-east-1'
    """)
except psycopg2.Error as e:
    print('Erro: issue creating table')
    print(e)


# In[ ]:


try:
    cur.execute("""
    copy event from 's3://dehou-bucket/allevents_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam:::role/redshift-s3-Access'
    delimiter '|'
    region 'us-east-1'
    """)
except psycopg2.Error as e:
    print('Erro: issue creating table')
    print(e)


# In[ ]:


try:
    cur.execute("""
    copy listing from 's3://dehou-bucket/listings_pipe.txt'
    credentials 'aws_iam_role=arn:aws:iam:::role/redshift-s3-Access'
    delimiter '|'
    region 'us-east-1'
    """)
except psycopg2.Error as e:
    print('Erro: issue creating table')
    print(e)


# In[ ]:


try:
    conn.close()
except psycopg2.Error as e:
    print(e)


# In[ ]:





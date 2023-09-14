from googleapiclient.discovery import build
import pymongo
from pprint import pprint
import psycopg2 as ps
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
#from sqlalchemy import create_engine

#connection to youtube API services 
def connect_api():
  API_key = 'AIzaSyDmXJiGUHLW5aAH4p3kLZ3tLvxev4uxMgY'
  api_service_name = "youtube"
  api_version = "v3"
  youtube = build(api_service_name,api_version,developerKey=API_key)
  return youtube

#Colecting the channel info using given Chanel name.
def channel_info(a):
  a = c_name
  request = yt.search().list(part="snippet",channelType="any",maxResults=1,q=c_name)
  responce = request.execute()
  channel_id = responce['items'][0]['snippet']['channelId']
  
  request = yt.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
  responce3 = request.execute()
  channel_id = responce['items'][0]['snippet']['channelId']
  Channel_Table = {"channel_id":responce['items'][0]['snippet']['channelId'],
                 "channel_name":responce['items'][0]['snippet']['channelTitle'],
                 "channel_views":int(responce3['items'][0]['statistics']['viewCount']),
                 "channel_description":str(responce3['items'][0]['snippet']['description']),
                 "published_at":responce['items'][0]['snippet']['publishTime'],
                 "channel_subscriber_count":int(responce3['items'][0]['statistics']['subscriberCount']),
                 "playlist_id":responce3['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                 }
  return Channel_Table

def playlist_info():
  a = c_name
  request = yt.search().list(part="snippet",channelType="any",maxResults=1,q=c_name)
  responce = request.execute()
  channel_id = responce['items'][0]['snippet']['channelId']
  request = yt.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
  responce2 = request.execute()
  request = yt.channels().list(part="snippet,contentDetails,statistics",id=channel_id)
  responce3 = request.execute()
  channel_id = responce['items'][0]['snippet']['channelId']
  channel_name = responce['items'][0]['snippet']['channelTitle']
  Playlist_Id = responce3['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  return Playlist_Id

def vedio_info(playlist_id):
  
  def time_duration(t):
        a = pd.Timedelta(t)
        b = str(a).split()[-1]
        return b
  request = yt.playlistItems().list(part="contentDetails", playlistId= playlist_id, maxResults=50)
  responce4 = request.execute()
  vedio = []
  a=0
  for i in responce4['items']:
    v = responce4['items'][a]['contentDetails']['videoId']
    vedio.append(v)
    a+=1

  a=[]
  for i in vedio:
    request = yt.videos().list(part="snippet,contentDetails,statistics", id=i)
    responce5 = request.execute()
    a.append(responce5['items'][0])
    #pprint(responce5['items'][0])
  z =[]
  for i in a:
    filtered_dict = {'video_id': i['id'],
                  'playlist_id':playlist_id,
                  'channel_id':i['snippet']['channelId'],
                  'video_name':i['snippet']['title'],
                  'video_description':i['snippet']['description'],
                  'published_date':i['snippet']['publishedAt'][0:10],
                  'published_time':i['snippet']['publishedAt'][11:19],
                  'view_count':i['statistics'].get('viewCount', 0),
                  'like_count':i['statistics'].get('likeCount', 0),
                  'favorite_count':i['statistics'].get('favoriteCount',0),
                  'comment_count':i['statistics'].get('commentCount',0),
                  'duration':time_duration(i['contentDetails']['duration']),
                  'thumbnail':i['snippet']['thumbnails'],
                  'caption_status':i['contentDetails']['caption']
                  }
    #pprint(filtered_dict)
    z.append(filtered_dict)
  #df2=pd.DataFrame(z)
  #df2
  return z


def vedio_id_info(playlist_id):
  request = yt.playlistItems().list(part="contentDetails", playlistId= playlist_id, maxResults=50)
  responce4 = request.execute()
  vedio_id = []
  a=0
  for i in responce4['items']:
    v = responce4['items'][a]['contentDetails']['videoId']
    vedio_id.append(v)
    a+=1
  return vedio_id

def comment_info():
  d=[]
  s =[]
  for i in Vedio_id_info:
    request = yt.commentThreads().list(part="id,snippet", videoId=i, maxResults=5)
    try:
      responce6 = request.execute()
    except:
      pass
    d.append(responce6['items'])
    for j in range(len(responce6['items'])):
      filtered_dict = {'comment_id': responce6['items'][j]['id'],
                     'video_id':responce6['items'][j]['snippet']['videoId'],
                     'comment_test':responce6['items'][j]['snippet']['topLevelComment']['snippet']['textDisplay'],
                     'comment_author':responce6['items'][j]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                     'comment_published_date':responce6['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                     'comment_published_time':responce6['items'][j]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19]
                     }
    #print(filtered_dict)
      s.append(filtered_dict)
  print(s)
  return s

def Final_Table():
  finalfile={'Channel_info':Channel_info,'Vedio_info':Vedio_info,'Comment_info':cmd}
  return finalfile

def get_chn_Table():
  kavi = pymongo.MongoClient("mongodb+srv://kavi:kaviarasan@kaviarasan.obhf2rg.mongodb.net/?retryWrites=true&w=majority")
  nosqldb=kavi["you_tube_data"]
  coll=nosqldb["channels_details"]
  task = []
  for i in coll.find({},{"_id":0,"Channel_info":1}):
    task.append(i['Channel_info'])
  #print(task)
  df2 = pd.DataFrame(task)
  return df2

def get_vedio_Table():
  kavi = pymongo.MongoClient("mongodb+srv://kavi:kaviarasan@kaviarasan.obhf2rg.mongodb.net/?retryWrites=true&w=majority")
  nosqldb=kavi["you_tube_data"]
  coll=nosqldb["channels_details"]
  task = []
  for i in coll.find({},{"_id":0,"Vedio_info":1}):
    for j in range(len(i['Vedio_info'])):
      task.append(i['Vedio_info'][j])

  df = pd.DataFrame(task)
  df[["thumbnail"]]=df["thumbnail"].apply(lambda i:pd.Series([i["default"]['url']]))
  return df

def get_cmd_Table():
  kavi = pymongo.MongoClient("mongodb+srv://kavi:kaviarasan@kaviarasan.obhf2rg.mongodb.net/?retryWrites=true&w=majority")
  nosqldb=kavi["you_tube_data"]
  coll=nosqldb["channels_details"]
  task = []
  for i in coll.find({},{"_id":0,"Comment_info":1}):
    for j in range(len(i['Comment_info'])):
      #print(i['Vedio_info'][j])
      task.append(i['Comment_info'][j])

  df1 = pd.DataFrame(task)
  return df1

def connecte_db():
  host_name = 'database-1.cjti1owjmpi2.ap-south-1.rds.amazonaws.com'
  dbname = 'youtube'
  port = '5432'
  username = 'postgres'
  password = '12345678'
  conn = None
  conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  cursor = conn.cursor()
  #print('Connected!')

def drop_tables():
  host_name = 'database-1.cjti1owjmpi2.ap-south-1.rds.amazonaws.com'
  dbname = 'youtube'
  port = '5432'
  username = 'postgres'
  password = '12345678'
  conn = None
  conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  cursor = conn.cursor()
  cursor.execute('DROP TABLE IF EXISTS channel')
  conn.commit()
  cursor.execute('DROP TABLE IF EXISTS video')
  conn.commit()
  cursor.execute('DROP TABLE IF EXISTS cmd')
  conn.commit()

def create_tables():
  host_name = 'database-1.cjti1owjmpi2.ap-south-1.rds.amazonaws.com'
  dbname = 'youtube'
  port = '5432'
  username = 'postgres'
  password = '12345678'
  conn = None
  conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  cursor = conn.cursor()
  cursor.execute("""CREATE TABLE IF NOT EXISTS channel(\
                                        channel_id 			varchar(255) primary key,\
                                        channel_name		varchar(255),\
                                        channel_views		BIGINT,\
                                        channel_description	text,\
                                        published_at	varchar(255),\
                                        channel_subscriber_count			BIGINT,\
                                        playlist_id				varchar(255))""")
  conn.commit()

  cursor.execute("""CREATE TABLE IF NOT EXISTS video(\
                                        video_id 			varchar(50) primary key,\
                                        playlist_id		varchar(50),\
                                        channel_id		varchar(50),\
                                        video_name	text,\
                                        video_description	text,\
                                        published_date			date,\
                                        published_time   time,\
                                        view_count				BIGINT,\
                                        like_count        BIGINT,\
                                        favorite_count    BIGINT,\
                                        comment_count     BIGINT,\
                                        duration          time,\
                                        thumbnail         varchar(150),\
                                        caption_status    varchar(10))""")
  conn.commit()

  cursor.execute("""CREATE TABLE IF NOT EXISTS cmd(\
                                        comment_id 			varchar(60),\
                                        video_id		varchar(50),\
                                        comment_test		text,\
                                        comment_author	text,\
                                        comment_published_date	date,\
                                        comment_published_time  time)""")
  conn.commit()

def insert_data_tables():
  host_name = 'database-1.cjti1owjmpi2.ap-south-1.rds.amazonaws.com'
  dbname = 'youtube'
  port = '5432'
  username = 'postgres'
  password = '12345678'
  conn = None
  conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  cursor = conn.cursor()
  
  cursor.executemany("insert into channel(channel_id,\
                     channel_name,\
                     channel_views,\
                     channel_description,\
                     published_at,\
                     channel_subscriber_count,\
                     playlist_id) values(%s,%s,%s,%s,%s,%s,%s)",
                     df.values.tolist())
  conn.commit()
  cursor.executemany("insert into video(video_id,\
                     playlist_id,\
                     channel_id,\
                     video_name,\
                     video_description,\
                     published_date,\
                     published_time,\
                     view_count,\
                     like_count,\
                     favorite_count,\
                     comment_count,\
                     duration,\
                     thumbnail,\
                     caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                     df1.values.tolist())
  conn.commit()
  cursor.executemany("insert into cmd(comment_id,\
                     video_id,\
                     comment_test,\
                     comment_author,\
                     comment_published_date,\
                     comment_published_time) values(%s,%s,%s,%s,%s,%s)",
                     df2.values.tolist())

  conn.commit()

def qus1():
  cursor.execute('select v.video_name,c.channel_name from video v join channel c on v.channel_id = c.channel_id')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['video_name', 'channel_name'])

def qus2():
  cursor.execute('select distinct channel.channel_name, count(distinct video.video_id) as total\
                  from video\
                  inner join channel on channel.channel_id = video.channel_id\
                  group by channel.channel_id\
                  order by total DESC')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['channel_name', 'total'])

def qus3():
  cursor.execute('select v.video_name,v.view_count,c.channel_name\
                 from video v\
                 join channel c\
                 on v.channel_id = c.channel_id\
                 order by v.view_count DESC\
                 FETCH NEXT 10 ROWS ONLY;')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['video_name', 'view_count', 'channel_name']) 

def qus4():
  cursor.execute('select video_name , comment_count\
                 from video')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['video_name', 'comment_count'])

def qus5():
  cursor.execute('select v.video_name,v.like_count,c.channel_name\
                 from video v\
                 join channel c\
                 on v.channel_id = c.channel_id\
                 where v.like_count = (select max(like_count) from video)')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['video_name', 'like_count', 'channel_name'])


def qus6():
  cursor.execute('select video_name , like_count from video')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['video_name', 'like_count'])

def qus7():
  cursor.execute('select channel_name,channel_views from channel')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['channel_name', 'channel_views'])
  
def qus9():
  cursor.execute('select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average\
                  from video\
                  inner join channel on channel.channel_id = video.channel_id\
                  group by channel.channel_id\
                  order by average DESC')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['channel_name', 'average'])

def qus8():
  cursor.execute("select distinct channel.channel_name, count(distinct video.video_id) as total\
                 from video\
                 inner join channel on channel.channel_id = video.channel_id\
                 where date_part('year', video.published_date) = 2023\
                 group by channel.channel_id\
                 order by total DESC")
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['channel_name', 'total'])


def qus10():
  cursor.execute('select video.video_name, video.comment_count, channel.channel_name\
                    from video\
                    inner join channel on channel.channel_id = video.channel_id\
                    group by video.video_id, channel.channel_name\
                    order by video.comment_count DESC\
                    limit 1')
  s = cursor.fetchall()
  return pd.DataFrame(s, columns=['video_name', 'comment_count', 'channel_name'])





st.title('YouTube Data Harvesting and Warehousing')


selected = option_menu(None, ["Home", "Lode Data In SQL", 'Select the Query'], 
    icons=['house', 'cloud-upload', "list-task", 'gear'], 
    menu_icon="cast", default_index=0, orientation="horizontal")
selected


if selected == 'Home':
  c_name = st.text_input("Enter the youtube channel name and press submit: ")
  step1 = st.button('Submit')
  if step1:
    st.write(f"Given channel name is {c_name}, please click Migrate Data to MongoDB ")
    yt = connect_api()
    Channel_info = channel_info('a')
    st.json(Channel_info)


  step2 = st.button('Migrate Data to MongoDB')
  if step2:
      yt = connect_api()
      Channel_info = channel_info('a')
      playlist_id = playlist_info()
      Vedio_info = vedio_info(playlist_id)
      Vedio_id_info = vedio_id_info(playlist_id)
      cmd = comment_info()
      final = Final_Table()
      kavi = pymongo.MongoClient("mongodb+srv://kavi:kaviarasan@kaviarasan.obhf2rg.mongodb.net/?retryWrites=true&w=majority")
      nosqldb=kavi["you_tube_data"]
      coll=nosqldb["channels_details"]
      coll.insert_one(final)
      st.write("Data Migrated to MongoDB successfuly")
      #st.json(Channel_info)

if selected == 'Lode Data In SQL':
  step3  = st.button('Lode Data In SQL')
  if step3:
    host_name = 'database-1.cjti1owjmpi2.ap-south-1.rds.amazonaws.com'
    dbname = 'youtube'
    port = '5432'
    username = 'postgres'
    password = '12345678'
    conn = None
    conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
    cursor = conn.cursor()
    df = get_chn_Table()
    df1 = get_vedio_Table()
    df2 = get_cmd_Table()
    
    drop_tables()
    create_tables()
    insert_data_tables()
    st.dataframe(df)


if selected =='Select the Query':
  step4 = st.subheader('Select the Query below')
  q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
  q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
  q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
  q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
  q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
  q6 = 'Q6-What is the total number of likes for each video with their corresponding video names?'
  q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
  q8 = 'Q8-What are the names of all the channels that have published videos in the 2023?'
  q9 = 'Q9-What is the average duration of all videos in each channel with corresponding channel names?'
  q10 = 'Q10-Which videos have the highest number of comments with their corresponding channel names?'

  host_name = 'database-1.cjti1owjmpi2.ap-south-1.rds.amazonaws.com'
  dbname = 'youtube'
  port = '5432'
  username = 'postgres'
  password = '12345678'
  conn = None
  conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  cursor = conn.cursor()

  query_option = st.selectbox('', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
  if query_option == q1:
    st.dataframe(qus1())
  elif query_option == q2:
    st.dataframe(qus2())
  elif query_option == q3:
    st.dataframe(qus3())
  elif query_option == q4:
    st.dataframe(qus4())
  elif query_option == q5:
    st.dataframe(qus5())
  elif query_option == q6:
    st.dataframe(qus6())
  elif query_option == q7:
    st.dataframe(qus7())
  elif query_option == q8:
    st.dataframe(qus8())
  elif query_option == q9:
    st.dataframe(qus9())
  elif query_option == q10:
    st.dataframe(qus10())



    

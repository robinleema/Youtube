import googleapiclient.discovery
import pandas as pd
import mysql.connector
import datetime
import isodate
import streamlit as st

#API Access:

api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyCYCa6JO4y0wEsUom0Tc6WENRxBwynV70o"
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)


#Streamlit la page create pantrathu:

st.set_page_config(layout="wide")
st.title(":violet[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
channel_id=st.text_input("***Enter the channel ID***")
# with st.sidebar:
#     st.header("SKILLS TO USE")
#     st.caption("Python code")
#     st.caption("Data Extract")
#     st.caption("Migrate to SQL")
#     st.caption("Pandas")


#Channel Information:
def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    data = {
            'channel_Name':response['items'][0]['snippet']['title'],
            #'channel_ID':response['items'][0]['id'],
            'channel_ID':channel_id,
            'subscribers':response['items'][0]['statistics']['subscriberCount'],
            'View':response['items'][0]['statistics']['viewCount'],
            'Total_Videos':response['items'][0]['statistics']['videoCount'],
            'Channel_Desc':response['items'][0]['snippet']['description'],
            'Playlist_ID':response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
    }
    return data


#Get_VIDEO ID'S DETAILS:
def videos_ids(channel_id):
    Video_Id=[]
    response=youtube.channels().list(
                id=channel_id,
                part='contentDetails').execute()
    if 'items' in response:
        playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        next_page_token=None

        while True:
            response2=youtube.playlistItems().list(
                        part='snippet',
                        playlistId=playlist_Id,
                        maxResults=50,
                        pageToken=next_page_token).execute()
            
            for i in response2.get('items',[]):
                Video_Id.append(i['snippet']['resourceId']['videoId'])
            next_page_token=response2.get('nextPageToken')

            if next_page_token is None:
                break
    return Video_Id


#Get_Videos Informations:

def video_info(video_Idinfo):
    video_data=[]
    for video_Id1 in video_Idinfo:
            request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=video_Id1
                )
            response_vinfo=request.execute()
            data={
                    'Channel_Name':response_vinfo['items'][0]['snippet']['channelTitle'],
                    'channel_ID':response_vinfo['items'][0]['snippet']['channelId'],
                    'Video_Name':response_vinfo['items'][0]['snippet']['title'],            
                    'Video_ID':response_vinfo['items'][0]['id'],
                    'Video_Description':response_vinfo['items'][0]['snippet'].get('description'),            
                    'Video_Definition':response_vinfo['items'][0]['contentDetails']['definition'],            
                    'Video_Caption':response_vinfo['items'][0]['contentDetails']['caption'],           
                    'Video_PubDate':response_vinfo['items'][0]['snippet']['publishedAt'].replace("T"," ").replace("Z",""),
                    'Video_Duration':str(isodate.parse_duration(response_vinfo['items'][0]["contentDetails"]['duration'])),
                    'Video_Viewcount':response_vinfo['items'][0]['statistics']['viewCount'],
                    'Video_likes':response_vinfo['items'][0]['statistics']['likeCount'],
                    'Video_Comment':response_vinfo['items'][0]['statistics'].get('commentCount'),
                    'Video_Favorite':response_vinfo['items'][0]['statistics']['favoriteCount'],
                    'Video_Tags':"".join(response_vinfo['items'][0]['snippet'].get('tags',[])),
                    'Video_Thumbnail':response_vinfo['items'][0]['snippet']['thumbnails']['default']['url']
                }
            video_data.append(data)
    
    return video_data


#Get_Comment Information:

def Comment_info(Total_video_ids):
    Comment_Data=[]
    try:
        for video_id in Total_video_ids:
            request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100
                )
            response_comment= request.execute()
            data={
                'Comment_Id':response_comment['items'][0]['id'],
                'video_id':response_comment['items'][0]['snippet']['videoId'],
                'Comment_Text':response_comment['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'],
                'Comment_Author':response_comment['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'Comment_Published':response_comment['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt'].replace("T"," ").replace("Z","")
            }
            Comment_Data.append(data)

    except:
        pass
    return Comment_Data


#Mysql Connection code:
import mysql.connector

mydb = mysql.connector.connect(
 host="127.0.0.1",
 user="root",
 password="Leema",
 database="youtube_data",
 port="3306"
 )

#print(mydb)
mycursor = mydb.cursor(buffered=True)


#MYSQL La Database Create Pantrathu:
#mycursor.execute("CREATE DATABASE youtube_data")

#MYSQL la Tables Create Pantrathu:
#drop_table='''drop table if exists channels'''
#mycursor.execute(drop_table)
#mydb.commit()
def create_table1():
    try:
        create_table='''create table if not exists channels(channel_Name varchar(255),
                                                            channel_ID varchar(255) primary key,
                                                                subscribers bigint,
                                                                View bigint,
                                                                Total_Videos int,
                                                                Channel_Desc text,
                                                                Playlist_ID varchar(80))'''
        mycursor.execute("ALTER table channels add index idx_name (name)")
        mycursor.execute(create_table)  
        mydb.commit()

    except:
            print("channel table already created") 


#Create ana table la Channel Details Insert Pantrathu:

def channel_insert(df1):
    for index,row in df1.iterrows():
        sql = '''INSERT INTO channels (channel_Name,
                                    channel_ID,
                                    subscribers,
                                    View,
                                    Total_Videos,
                                    Channel_Desc,
                                    Playlist_ID ) VALUES (%s, %s,%s,%s,%s,%s,%s)'''
        val = (row["channel_Name"],
                row["channel_ID"],
                row["subscribers"],
                row["View"],
                row["Total_Videos"],
                row["Channel_Desc"],
                row["Playlist_ID"])

        mycursor.execute(sql, val)

        mydb.commit()

        print(mycursor.rowcount, "record inserted.")


#Video Table Create:
#drop_table='''drop table if exists videos'''
#mycursor.execute(drop_table)
#mydb.commit()

def create_table2():
    try:
        create_table='''create table if not exists Videos(Channel_Name varchar(255),
                                                            channel_ID varchar(255),
                                                            Video_Name varchar(255),
                                                            Video_ID varchar(255) primary key,
                                                            Video_Description text,
                                                            Video_Definition varchar(255),
                                                            Video_Caption varchar(255),
                                                            Video_PubDate datetime,
                                                            Video_Duration time,
                                                            Video_Viewcount bigint,
                                                            Video_likes bigint,
                                                            Video_Comment int,
                                                            Video_Favorite int,
                                                            Video_Tags text,
                                                            Video_Thumbnail varchar(255))'''
        mycursor.execute(create_table)  
        mydb.commit()
    except:
        print("video table already created")


#SQL la video details insert pantrathu:

def video_insert(df2):
    for index,row in df2.iterrows():
        sql = '''INSERT INTO videos(Channel_Name,
                                    channel_ID,
                                    Video_Name,
                                    Video_ID,
                                    Video_Description,
                                    Video_Definition,
                                    Video_Caption,
                                    Video_PubDate,
                                    Video_Duration,
                                    Video_Viewcount,
                                    Video_likes,
                                    Video_Comment,
                                    Video_Favorite,
                                    Video_Tags,
                                    Video_Thumbnail) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        val =(row['Channel_Name'],
            row['channel_ID'],
            row['Video_Name'],
            row['Video_ID'],
            row['Video_Description'],
            row['Video_Definition'],
            row['Video_Caption'],
            row['Video_PubDate'],
            row['Video_Duration'],
            row['Video_Viewcount'],
            row['Video_likes'],
            row['Video_Comment'],
            row['Video_Favorite'],
            row['Video_Tags'],
            row['Video_Thumbnail'])

        mycursor.execute(sql, val)
        mydb.commit()


#Comments Table Create:
# drop_table='''drop table if exists Comments'''
# mycursor.execute(drop_table)
# mydb.commit()

def create_table3():
    try:
        create_table='''create table if not exists Comments(Comment_Id varchar(255) primary key,
                                                            video_id varchar(100),
                                                            Comment_Text Text,
                                                            Comment_Author varchar(255),
                                                            Comment_Published timestamp)'''
            
        mycursor.execute(create_table)  
            
        mydb.commit()
    except:
        print("Comment table already create")


#Comments INSERT in SQL Table:
def comment_insert(df3):
    for index,row in df3.iterrows():
        sql = '''INSERT INTO comments(Comment_Id,
                                    video_id,
                                    Comment_Text,
                                    Comment_Author,
                                    Comment_Published)
                                    VALUES (%s,%s,%s,%s,%s)'''

        val =(row['Comment_Id'],
            row['video_id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published'])

        mycursor.execute(sql, val)

        mydb.commit()


#Streamlit la data show pantrathu:

if channel_id and st.button("INSERT DATA"):
    st.success("data insert successfully")
    channel_details=channel_data(channel_id)
    all_Video_Id=videos_ids(channel_id)
    Comment_Details=Comment_info(all_Video_Id)
    video_Details=video_info(all_Video_Id)
    df1=pd.DataFrame(channel_details,index=[0])
    df2=pd.DataFrame(video_Details)
    df3=pd.DataFrame(Comment_Details)
    create_table1()
    channel_insert(df1)
    create_table2()
    video_insert(df2)
    create_table3()
    comment_insert(df3)


#Streamlit Ah convert pantrathu:
def view_channels_table():
    mycursor.execute("SELECT * FROM channels")

    result=mycursor.fetchall()

    channel_st=st.write(pd.DataFrame(result,columns=["channel_Name",
                               "channel_ID",
                               "subscribers",
                               "View",
                               "Total_Videos",
                               "Channel_Desc",
                               "Playlist_ID"]))

    return channel_st

def view_videos_table():
    mycursor.execute("SELECT * FROM videos")

    result=mycursor.fetchall()

    videos_st=st.write(pd.DataFrame(result,columns=["Channel_Name",
                                                "channel_ID",
                                                "Video_Name",
                                                "Video_ID",
                                                "Video_Description",
                                                "Video_Definition",
                                                "Video_Caption",
                                                "Video_PubDate",
                                                "Video_Duration",
                                                "Video_Viewcount",
                                                "Video_likes",
                                                "Video_Comment",
                                                "Video_Favorite",
                                                "Video_Tags",
                                                "Video_Thumbnail"]))

    return videos_st

def view_comments_table():
    mycursor.execute("SELECT * FROM comments")

    result=mycursor.fetchall()

    comments_st=st.write(pd.DataFrame(result,columns=["Comment_Id",
                                  "video_id",
                                  "Comment_Text",
                                  "Comment_Author",
                                  "Comment_Published"]))

    return comments_st



#st.button("INSERT",type="primary")
with st.sidebar:
    show_table=st.radio("***SELECT TABLE FOR VIEW***",(":rainbow[CHANNELS]",":rainbow[VIDEOS:movie_camera:]",":rainbow[COMMENTS]"))
        
    if show_table==":rainbow[CHANNELS]":
        view_channels_table()

    elif show_table==":rainbow[VIDEOS:movie_camera:]":
        view_videos_table()

    elif show_table==":rainbow[COMMENTS]":
        view_comments_table()


#Questions:
with st.sidebar:
    question=st.selectbox("**Select your question**",("1.What are the names of all the videos and Corresponding channels",
                                                "2.Which channels have the most number of videos",
                                                "3.What are the top 10 most viewed videos",
                                                "4.How many comments were made on each video",
                                                "5.Which videos have the highest number of likes",
                                                "6.What is the total number of likes",
                                                "7.What is the total number of views",
                                                "8.Published videos in the year 2022",
                                                "9.What is the average duration of all videos",
                                                "10.Which videos have the highest number of comments"))

#Answers-01:
if question=="1.What are the names of all the videos and Corresponding channels":
    query1='''select Video_Name as videos,Channel_Name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    table1=mycursor.fetchall()
    df=pd.DataFrame(table1,columns=["Video Name","Channel Name"])
    st.write(df)

#Answer-02:
elif question=="2.Which channels have the most number of videos":
    query2='''select Channel_Name as Channelname, Total_Videos as number_videos from channels
                order by Total_Videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    table2=mycursor.fetchall()
    df2=pd.DataFrame(table2,columns=["Channel Name","Number of videos"])
    st.write(df2)

#Answer-03:
elif question=="3.What are the top 10 most viewed videos":
    query3='''select Video_Viewcount as Views, Channel_Name as Channelname,Video_Name as Videoname from videos
            where Video_Viewcount is not null order by Views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    table3=mycursor.fetchall()
    df3=pd.DataFrame(table3,columns=["Views","Channel Name","Video Name"])
    st.write(df3)

#Answer-04:
elif question=="4.How many comments were made on each video":
    query4='''select Video_Comment as no_comments,Video_Name as videoname from videos where Video_Comment is not null'''
    mycursor.execute(query4)
    mydb.commit()
    table4=mycursor.fetchall()
    df4=pd.DataFrame(table4,columns=["No Of Comments","Video Name"])
    st.write(df4)

#Answer-05:
elif question=="5.Which videos have the highest number of likes":
    query5='''select Video_Name as Videoname,Channel_Name as Channlename,Video_likes as likecount
                    from videos where Video_likes is not null order by likecount desc'''
    mycursor.execute(query5)
    mydb.commit()
    table5=mycursor.fetchall()
    df5=pd.DataFrame(table5,columns=["Videoname","Channelname","likecount"])
    st.write(df5)

#Answer-06:
elif question=="6.What is the total number of likes":
    query6='''select  Video_likes as Likecount, Video_Name as videoname from videos'''
    mycursor.execute(query6)
    mydb.commit()
    table6=mycursor.fetchall()
    df6=pd.DataFrame(table6,columns=["Likecount","videoname"])
    st.write(df6)

#Answer-07:
elif question=="7.What is the total number of views":
    query7='''select channel_Name as Channelname,View as Totalviews from channels'''
    mycursor.execute(query7)
    table7=mycursor.fetchall()
    df7=pd.DataFrame(table7,columns=["Channelname","Totalviews"])
    st.write(df7)

#Answer-08:
elif question=="8.Published videos in the year 2022":
    query8='''select Video_Name as Videoname,Video_PubDate as videodate,Channel_Name as Channelname from videos
            where extract(year from Video_PubDate)=2022'''
    mycursor.execute(query8)
    table8=mycursor.fetchall()
    df8=pd.DataFrame(table8,columns=["Videoname","videodate","Channelname"])
    st.write(df8)

#Answer-09:
elif question=="9.What is the average duration of all videos":
    query9='''select Channel_Name as Channelname,AVG(Video_Duration) as Average_duration from videos group by Channel_Name'''
    mycursor.execute(query9)
    table9=mycursor.fetchall()
    df9=pd.DataFrame(table9,columns=["Channelname","Average_duration"])
#list ah create paniruke:
    T9=[]
    for index,row in df9.iterrows():
        Channel_name=row["Channelname"]
        Average_duration=row["Average_duration"]
        Average_duration_str=str(Average_duration)
        T9.append(dict(Channelname=Channel_name,avgduration=Average_duration_str))
    df=pd.DataFrame(T9)
    st.write(df)

#Answer-10:
elif question=="10.Which videos have the highest number of comments":
    query10='''select Video_Name as Videoname, Channel_Name as Channelname, Video_Comment as Comments from videos where Video_Comment 
            is not null order by Video_Comment desc'''
    mycursor.execute(query10)
    table10=mycursor.fetchall()
    df10=pd.DataFrame(table10,columns=["Videoname","Channelname","High Comments"])
    st.write(df10)
import streamlit as st  # creating web app
from googleapiclient.discovery import build  # to get youtube APIs
from pymongo import MongoClient  # mongodb connect
import mysql.connector  # mySQL connect
import re
import pandas as pd  # dataframe
youtube_data = {}  # final output data containing all youtube datas (Channel, Playlist, Video)
youtube_comment = {}  # final output data containing comments
mongodb_comment = []


def get_youtube_data(channelid):  # setting an connection to YouTube and get datas using youtube data api
    api_key = ""  # connecting to YouTube using unique API key
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)  # setting up connection to YouTube

    playlist = get_channel_data(youtube, channelid)  # getting channel data which also returns playlist_id
    get_playlist_details(youtube, channelid)  # getting list of playlists and its details
    video_ids = get_video_id(youtube, playlist)  # getting list of video ids

    for v_index, video_id in enumerate(video_ids):  # iterating through list of video ids one by one
        comment_id = video_id
        video_detail = get_video_details(youtube, video_id)  # function returns details of each video
        video_index = v_index + 1  # indexing purpose
        youtube_data[f'video_id_{video_index}'] = video_detail  # appending video data to final variable youtube_data
        all_comments = get_comments(youtube, comment_id)  # function returns dict of comments for a video
        # creating a nested dict to store datas for respective video , the datas has the list of comments for that video
        youtube_comment[f'comments {video_index}'] = {}

        for c_index, comment in enumerate(all_comments):  # iterating through each comments in a video
            comment_name = f'comment_id_{c_index + 1}'
            if comment != '':
                youtube_comment[f'comments {video_index}'][comment_name] = comment  # appending comment data
        mongodb_comment.append(youtube_comment[f'comments {video_index}'])  # for mongodb import

def get_channel_data(youtube, channelid):  # use YouTube data api to return the channel details for a particular channel
    request = youtube.channels().list(  # channel_id is the main argument to get channel details
        part="snippet,contentDetails,statistics",
        id=channelid
    )
    response = request.execute()  # executing youtube api request
    for item in response['items']:  # iterating through data
        channel_val = {'Channel_Name': item['snippet']['title'],
                       'Channel_Id': channelid,
                       'Subscription_Count': item['statistics']['subscriberCount'],
                       'Channel_Views': item['statistics']['viewCount'],
                       'Channel_Description': item['snippet']['description'],
                       'Playlist_Id': item['contentDetails']['relatedPlaylists']['uploads']
                       }
        youtube_data['Channel_Name'] = channel_val  # appending fetched data to final variable
        return channel_val['Playlist_Id']  # returning playlist_id


def get_playlist_details(youtube, channelid):  # uses youtube data api to retrieve playlist data
    request = youtube.playlists().list(  # channel_id is the main argument, part is the part of data to be returned
        part="snippet,contentDetails",
        channelId=channelid,
        maxResults=50
    )
    response = request.execute()  # executing request

    for index, item in enumerate(response['items']):
        playlist_name = f"playlist_id_{index + 1}"
        details = {'Playlist_id': item['id'],
                   'Channel_id': item['snippet']['channelId'],
                   'Playlist_name': item['snippet']['title']
                   }
        youtube_data[playlist_name] = details  # appending fetched playlist data to final variable


def get_video_id(youtube, playlistid):  # use youtube data api to fetch video ids
    global playlist_id
    playlist_id = playlistid
    videolist = []  # creating empty to list to store video ids

    request = youtube.playlistItems().list(  # playlistid is the main argument to retrieve video ids
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()  # executing request

    for item in response['items']:
        videoid = item['contentDetails']['videoId']
        videolist.append(videoid)  # appending video id to list
    token = response.get('nextPageToken', None)  # apis generally fetch only 50 data to get all data fetch nextPageToken

    while token is not None:  # when next page is not null loop to next page
        # playlistid is the main argument, pass pageToken as argument to get next page data
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlistid,
            maxResults=50,
            pageToken=token
        )
        response = request.execute()  # executing youtube request

        for item in response['items']:
            videoid = item['contentDetails']['videoId']
            videolist.append(videoid)  # appending video ids to the list
        token = response.get('nextPageToken', None)  # get nextPageToken if available loop or abort
    return videolist  # return the list of video_ids


def get_video_details(youtube, videoid):  # get details of indivial videos
    video_details = ''
    request = youtube.videos().list(  # videoid the main argument to get details
        part="snippet,contentDetails,statistics",
        id=videoid,
        maxResults=50
    )
    response = request.execute()  # executing request

    for item in response['items']:
        statistics = item['statistics']
        duration = item['contentDetails']['duration']
        duration_df = pd.Timedelta(duration)
        duration_df = str(duration_df)[-8:]
        video_details = {
            'video_id': videoid,
            'playlist_id': playlist_id,
            'video_name': item['snippet']['title'],
            'video_description': item['snippet']['description'],
            'published_date': re.sub('[TZ]', ' ', item['snippet']['publishedAt']).strip(),
            'view_count': statistics.get('viewCount', '0'),
            'like_count': statistics.get('likeCount', '0'),
            'dislike_count': statistics.get('dislikeCount', '0'),
            'favorite_count': statistics.get('favoriteCount', '0'),
            'comment_count': statistics.get('commentCount', '0'),
            'duration': duration_df,
            'thumbnail': item['snippet']['thumbnails']['default']['url'],
            'caption_status': item['contentDetails']['caption']
        }
    return video_details  # return the video details for individual videos


def get_comments(youtube, video_id):  # get comments list for individual videos
    comments_data = []
    try:
        request = youtube.commentThreads().list(  # video_id is the main argument to get comment details
            part="snippet,replies",
            videoId=video_id,
            maxResults=50,
        )
        response = request.execute()  # executing request
    except:
        comments_data = ''
        return comments_data
    for index, item in enumerate(response['items']):
        details = dict(comment_id=item['id'], video_id=video_id,
                       comment_text=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                       comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                       comment_published_date=re.sub('[TZ]', ' ', item['snippet']['topLevelComment']['snippet']
                       ['publishedAt']).strip())
        comments_data.append(details)  # appending comments to the list
    token = response.get('nextPageToken', None)  # get nextPageToken
    while token is not None:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=50,
                pageToken=token
            )
            response = request.execute()
        except:
            pass
        for index, item in enumerate(response['items']):
            details = dict(comment_id=item['id'], video_id=video_id,
                           comment_text=item['snippet']['topLevelComment']['snippet']['textOriginal'],
                           comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                           comment_published_date=re.sub('[TZ]', ' ', item['snippet']['topLevelComment']['snippet']
                           ['publishedAt']).strip())
            comments_data.append(details)
            token = response.get('nextPageToken', None)

    return comments_data  # return list of comments for individual videos


def mongodb_importdata(channelid):  # function to import data into mongodb
    # if collection and document exists for channel_id in mongodb then replace else insert
    if channelid in collection_list:
        mycol.delete_many({})
        mycol.insert_one(youtube_data)  # replace document with youtube_data fetched recently
        mycol.insert_many(mongodb_comment)
    else:
        mycol.insert_one(youtube_data)  # if collection does not exist then create one and insert youtube_data
        mycol.insert_many(mongodb_comment)

def mongodb_exportdata(sqlid):  # extracting unstructured data from mongodb
    my_col = mydb[f'{sqlid}']  # sql_id is the channel_id that is selected for importing to sql
    data_extract = my_col.find()  # extracting data with find function which will return all documents in the collection
    # data has to be converted to a list because find() will return mongodb cursor
    extracted_data_list = list(data_extract)
    return extracted_data_list  # list of all documents in the collection


def mysql_createtable():  # To create a database and create 4 tables in database to store data
    my_cursor = my_db.cursor()  # cursor in mysql to type sql commands as in shell
    my_cursor.execute('CREATE DATABASE IF NOT EXISTS youtube_db')  # to create a database youtube_data if not exists
    my_db.database = 'youtube_db'  # connecting to database youtube_data after creation

    # creating TABLE Channel, Comment, Playlist, Video in DATABASE youtube_data with the following columns and datatype
    my_cursor.execute("CREATE TABLE if not exists Channel (channel_name VARCHAR(255), channel_id VARCHAR(255), "
                      "subscription_count INT, channel_views INT, channel_description TEXT, playlist_id VARCHAR(255))")

    my_cursor.execute("CREATE TABLE if not exists Playlist (playlist_id VARCHAR(255), channel_id VARCHAR(255), "
                      "playlist_name VARCHAR(255))")

    my_cursor.execute("CREATE TABLE if not exists Video (video_id VARCHAR(255), playlist_id VARCHAR(255), video_name"
                      " VARCHAR(255), video_description TEXT, published_date DATETIME, view_count INT, like_count "
                      "INT, dislike_count INT,favorite_count INT, comment_count INT, duration VARCHAR(255), "
                      "thumbnail VARCHAR(255), caption_status VARCHAR(255))")

    my_cursor.execute("CREATE TABLE if not exists Comment (comment_id VARCHAR(255), video_id VARCHAR(255), comment_text"
                      " TEXT, comment_author VARCHAR(255) , comment_published_date DATETIME)")


# to structure the data extracted from mongodb and import structured data to the respective tables created in database
def mysql_importdata(extracted_data_list):
    my_cursor = my_db.cursor()  # cursor to execute sql commands as in shell
    # extracted_data contains the list of documents. iterating 1 by 1 to extract document

    for items in extracted_data_list:

        # a doc is a dictionary with all the keys i.e. youtube_data variable that is imported to mongodb
        key_list = items.keys()  # will return the list of dictionary keys in a doc i.e. 'Channel_Name', 'Playlist_id_1'
        for item in key_list:  # iterating through all the keys 1 by 1 to load all dictionary data to tables created

            # item is just a string containing the str(key)
            if item.startswith('Channel'):  # checks whether the key has string 'Channel'
                channel_docs = items[item]  # items is the dictionary and item is the key returns another dictionary
                placeholders = ', '.join(['%s'] * len(channel_docs))  # %s,%s,%s,%s,%s,%s i.e. %s * len of columns
                columns = ",".join(channel_docs.keys())  # channel_docs is a dict. Joining all keys with ','
                sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('Channel', columns, placeholders)  # insert data to table
                my_cursor.execute(sql, list(channel_docs.values()))  # feed data to columns
                my_db.commit()  # committing database

            elif item.startswith('playlist'):  # for playlist table data
                playlist_docs = items[f'{item}']
                placeholders = ', '.join(['%s'] * len(playlist_docs))
                columns = ",".join(playlist_docs.keys())
                # inserting dictionary datas to table iterating through each item
                sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('Playlist', columns, placeholders)
                my_cursor.execute(sql, list(playlist_docs.values()))
                my_db.commit()

            elif item.startswith('video'):  # for video table data
                video_docs = items[f'{item}']
                placeholders = ', '.join(['%s'] * len(video_docs))
                columns = ",".join(video_docs.keys())
                sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('Video', columns, placeholders)
                my_cursor.execute(sql, list(video_docs.values()))
                my_db.commit()

            elif item.startswith('comment'):  # for comments table data
                    comment_docs = items[item]
                    placeholders = ', '.join(['%s'] * len(comment_docs))
                    columns = ",".join(comment_docs.keys())
                    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ('Comment', columns, placeholders)
                    my_cursor.execute(sql, list(comment_docs.values()))
                    my_db.commit()


def get_querylist():  # query list displayed in streamlit
    query1 = 'What are the names of all the videos and their corresponding channels?'
    query2 = 'Which channels have the most number of videos, and how many videos do they have?'
    query3 = "What are the top 10 most viewed videos and their respective channels?"
    query4 = "How many comments were made on each video, and what are their corresponding video names?"
    query5 = "Which videos have the highest number of likes, and what are their corresponding channel names?"
    query6 = "What is the total number of likes and dislikes for each video, and what are their corresponding" \
             " video names?"
    query7 = "What is the total number of views for each channel, and what are their corresponding channel names?"
    query8 = "What are the names of all the channels that have published videos in the year 2022?"
    query9 = "What is the average duration of all videos in each channel, and what are their corresponding " \
             "channel names?"
    query10 = "Which videos have the highest number of comments, and what are their corresponding channel names?"
    query = [query1, query2, query3, query4, query5, query6, query7, query8, query9, query10]
    return query


def get_queryoutput(queryid):  # querying SQL database for results
    df = ''
    my_cursor = my_db.cursor()
    querylist = get_querylist()
    if queryid == querylist[0]:
        my_cursor.execute('SELECT distinct video.video_id,video.video_name,channel.channel_name '
                          'FROM youtube_db.video '
                          'JOIN youtube_db.channel '
                          'ON video.playlist_id = channel.playlist_id')
        my_result = my_cursor.fetchall()    # fetching select results
        df = pd.DataFrame(my_result, columns=['Video id', 'Video Name', 'Channel'])  # converting results to dataframe
        df.pop('Video id')  # removing datas not needed

    if queryid == querylist[1]:
        my_cursor.execute('SELECT video.playlist_id, channel.channel_name,COUNT(distinct video.video_id) '
                          'FROM youtube_db.video '
                          'inner JOIN youtube_db.channel '
                          'ON video.playlist_id = channel.playlist_id '
                          'group by video.playlist_id,channel.channel_name '
                          'order by COUNT(distinct video.video_id) desc limit 1')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Playlist id', 'Channel', 'Total Videos'])
        df.pop('Playlist id')

    if queryid == querylist[2]:
        my_cursor.execute('SELECT distinct video_id,video_name,max(view_count),channel_name '
                          'FROM youtube_db.video '
                          'JOIN youtube_db.channel '
                          'ON video.playlist_id = channel.playlist_id '
                          'GROUP BY video_id,video_name,channel_name '
                          'ORDER BY max(view_count) desc LIMIT 10')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Video id', 'Video Name', 'View count', 'Channel'])
        df.pop('Video id')
        df.pop('View count')

    if queryid == querylist[3]:
        my_cursor.execute('SELECT distinct video_id,video_name,max(comment_count) '
                          'FROM youtube_db.video '
                          'GROUP BY video_id,video_name')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Video id', 'Video Name', 'Comment count'])
        df.pop('Video id')

    if queryid == querylist[4]:
        my_cursor.execute('SELECT distinct video_id,video_name,max(like_count),channel_name '
                          'FROM youtube_db.video '
                          'JOIN youtube_db.channel '
                          'ON video.playlist_id = channel.playlist_id '
                          'GROUP BY video_id,video_name,channel_name '
                          'ORDER BY max(like_count) desc LIMIT 1')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Video id', 'Video Name', 'Like count', 'Channel'])
        df.pop('Video id')
        df.pop('Like count')

    if queryid == querylist[5]:
        my_cursor.execute('SELECT distinct video_id,video_name,max(like_count),max(dislike_count) '
                          'FROM youtube_db.video '
                          'group by video_id,video_name')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Video id', 'Video Name', 'Like count', 'Dislike count'])
        df.pop('Video id')

    if queryid == querylist[6]:
        my_cursor.execute('SELECT distinct channel_id,channel_name,max(channel_views) '
                          'FROM youtube_db.channel '
                          'GROUP BY channel_id,channel_name')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Channel id', 'Channel Name', 'Channel Views'])
        df.pop('Channel id')

    if queryid == querylist[7]:
        my_cursor.execute("SELECT count(distinct video.video_id),channel.channel_name,video.playlist_id "
                          "FROM youtube_db.video "
                          "JOIN youtube_db.channel "
                          "ON video.playlist_id = channel.playlist_id "
                          "WHERE video.published_date between '2022-01-01 00:00:00' and '2022-12-31 23:59:59' "
                          "group by video.playlist_id,channel.channel_name")
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Video id', 'Channel Name', 'Video Playlist id'])
        df.pop('Video id')
        df.pop('Video Playlist id')

    if queryid == querylist[8]:
        my_cursor.execute('SELECT video.playlist_id,channel_name,sec_to_time(avg(time_to_sec(duration))) '
                          'FROM youtube_db.video '
                          'JOIN youtube_db.channel '
                          'ON video.playlist_id= channel.playlist_id '
                          'group by video.playlist_id,channel_name')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Playlist id', 'Channel Name', 'Average duration'])
        df['Average duration'] = [str(item)[-15:][0:8] for item in df['Average duration']]
        df.pop('Playlist id')

    if queryid == querylist[9]:
        my_cursor.execute('SELECT distinct video_id,video_name,max(comment_count),channel_name '
                          'FROM youtube_db.video '
                          'JOIN youtube_db.channel '
                          'ON video.playlist_id = channel.playlist_id '
                          'GROUP BY video_id,video_name,channel_name '
                          'ORDER BY max(comment_count) desc LIMIT 1')
        my_result = my_cursor.fetchall()
        df = pd.DataFrame(my_result, columns=['Video id', 'Video Name', 'Comment count', 'Channel'])
        df.pop('Video id')
        df.pop('Comment count')

    return df  # returns result

# Streamlit application configurations
st.set_page_config(layout='wide')
message = ':film_projector:'
st.title(f'{message} Youtube Harvesting & Warehousing')  # main header
st.subheader('Created by Balavignesh')  # sub header for streamlit application

col1, col2 = st.columns([4, 1])  # dividing streamlit application frame to two columns with 2:1 ratio
# Text box to enter channel id input
channel_id = st.text_input(label='Enter the channel id', placeholder="Enter the youtube channel id")
mongodb_button = st.button('Get Data', help='Click to fetch and load mongodb with youtube datas')

# Connecting to mongodb, creating database youtube_db and creating a collection for each channel_id
connection_string = "mongodb+srv://<username>:<password>@cluster0.wcdutss.mongodb.net/"
myclient = MongoClient(connection_string)
mydb = myclient["youtube_db"]

options = []  # creating a list of options
collection_list = mydb.list_collection_names()
if channel_id != '':
    mycol = mydb[f'{channel_id}']

# Connecting to mysql
my_db = mysql.connector.connect(host="localhost", user="username", password="password")

# creating streamlit selection box with list of channel ids already populated in mongodb
for collection_id in collection_list:
    options.append(collection_id)  # list of channel_ids i.e. collections is provided as an option to upload to SQL
# channel_id that is to be updated to SQL
frame1 = st.container()
st.divider()
sql_id = st.selectbox('Select the Channel id to upload to mySQL', options, key="sqlid")
sql_button = st.button('Upload Data', help='Click to Upload data to SQL')  # updates data to sql for the selected id
frame2 = st.container()
st.divider()
query_id = st.selectbox("FAQ's", options=get_querylist())  # list of queries
query_button = st.button('Submit query', help='Select to get data')  # fetches query output


# clicking on get data button will fetch data from YouTube using YouTube data apis and move it to mongodb
if mongodb_button:
    if channel_id == '':
        frame1.error("Please enter a valid channel id")
    else:
        get_youtube_data(channel_id)  # to scrap necessary data and store it in final dictionary youtube_data
        mongodb_importdata(channel_id)  # to import unstructured data in variable youtube_data to mongodb
        st.experimental_rerun()

if channel_id in options:  # checks if collection is created
    frame1.success('Data is stored in MongoDB')

# clicking on upload data button will get data from mongodb convert it to structured data and load it to mySQL database
if sql_button:
    extracted_data = mongodb_exportdata(sql_id)  # getting the unstructured data from mongodb
    mysql_createtable()  # creating 4 tables in sql database
    mysql_importdata(extracted_data)  # import the structured data to respective mysql tables
    frame2.success('Data migrated to SQL')

if query_button:
    query_output = get_queryoutput(query_id)    # returns sql search results
    st.table(query_output)  # displaying output as table in streamlit

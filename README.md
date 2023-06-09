
Youtube Data Scraping


## Introduction

YouTube Data Scraper code allows the user to fetch data from YouTube. With the help of youtube Data API datas can be fetched from youtube. The information from a channel are collected such as number of videos, their names, number of likes, number of comments and are used for analysis.
## Features

The YouTube Data Scraper offers a wide range of features which helps you extract and analyze data from YouTube. Some of the key features include:

Retrieve channel data: Get detailed information about YouTube channels such as subscriber count, view count, video count, and  extract data such as video title, description, duration, view count, like count, dislike count, and publish date for individual videos.

Analyzing comments: Get comments made on YouTube videos and perform analysis.

Get Playlist Information: 
Details of the playlist in the channel can be extracted. This feature is particularly useful when you want to analyze playlists and their contents.

Data storage: Store the collected YouTube data in a Mongodb database for easy retrieval and future reference.


## Technologies used

Python

Python and yourtube API has been used to collect data from youtube.The complete project has been implemented using python. Libraries such as Pandas, Numpy were used.

Mongo Db

Mongo db was used as the database for the project and the collected data was stored in Mongodb cloud server. 

SQL

SQL was used to fetch all the data stored in mongo db

Streamlit

Streamlit was used for visualtization and all the results were run in streamlit.
## Working of the project

Enter the channel id in text box under the header Enter the channel id. Once you Input the Channel Id, click on Get data in order to retrive data from Youtube API.  The data automatically goes to Mongo db. Select the channel name from the dropdown Select the Channel id to upload to mySQL and click upload data to import data into PostgreSQL. You can alayze the data using the FAQ's dropdown and get the corresponding result.
## License
The YouTube Data Scraper is released under the MIT License. Feel free to modify and use the code according to the terms of the license.


## Conclusion
The project provides a convenient way to fetch and analyze data from YouTube channels. The data can be used to obtain useful insights of various youttube channels. 
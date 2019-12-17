# Data Modeling With Apache Cassandra

This repository contains a [Jupyter Notebook](./Project_1B_Project_Template.ipynb) that demonstrates how data modeling in Apache Cassandra looks like.
Data from various CSV logfiles are combined and used to populate three tables in Apache Cassandra, which are modeled in a way that they can answer these questions:

1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

All three tables are optimized for one of the queries each and demonstrate how partition and clustering keys must be used in order to be able to store the data and answer those queries efficiently.

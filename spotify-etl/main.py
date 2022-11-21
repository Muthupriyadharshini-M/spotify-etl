# Py code to extract an user's top songs, validate and store it in DB
# Steps
# 1. Extract from Spotify using it's APIs
# 2. Validate or Transform
# 3. Store or Load in SQLite DB

import requests
import json
import datetime
import time
import pandas as pd
import sqlalchemy
import sqlite3

## Extraction 
# Credentials
TOKEN = "" # Spotify token
USERNAME = "" # Spotify username
DATABASE_LOCATION= "" # Path where the SQLite DB resides

# Header
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": "Bearer "+ TOKEN,
    "Cache-Control": "no-cache"
}

# Generate timestamp
today = datetime.datetime.now()
today = today.replace(hour=0,minute=0,second=0,microsecond=0)
beforeAWeek = today - datetime.timedelta(days=7)
beforeAWeekEpoch = int(beforeAWeek.timestamp())*1000

response = requests.get("https://api.spotify.com/v1/me/player/recently-played?after=" + str(beforeAWeekEpoch),headers=headers)
if response.status_code != 200:
    raise Exception("The API didn't return 200")

jsonData = response.json()

# Parse and identify required data
artist = [];
album = [];
playedAt = [];
songName = [];

for item in jsonData["items"]:
    artist.append(item["track"]["album"]["artists"][0]["name"])
    album.append(item["track"]["album"]["name"])
    songName.append(item["track"]["name"])
    playedAt.append(item["played_at"])

dataDict = {
    "artists": artist,
    "albums": album,
    "playedAts": playedAt,
    "songNames": songName
}

dataFrame = pd.DataFrame(dataDict,index=None)

# Validation

def validation(dF):

    # Check if empty
    if dF.empty:
        print("Data frame is empty")

    # Primary key violation
    if pd.Series(dF["playedAts"]).is_unique:
        pass
    else:
        raise Exception("Primary key uniqueness is compromised. Validation failed")

    # Time violation
    playedAts = dF["playedAts"].tolist()
    for playedAt in playedAts:
        if datetime.datetime.strptime(playedAt[0:10],"%Y-%m-%d") < beforeAWeek:
            raise Exception("Records older than a week were found. Validation failed")

    # Check for null values
    if dF.isnull().values.any():
        raise Exception("Null values found") 

    return True           


if validation(dataFrame):
    print("Validation stage passed")
else:
    print("Error during data validation")

# Load the data to SQLite
sqlEngine = sqlalchemy.create_engine("sqlite:///" + DATABASE_LOCATION)
connection = sqlite3.connect(DATABASE_LOCATION)
cursor = connection.cursor()

# Create table
sql_query = """
    CREATE TABLE IF NOT EXISTS Dharshini_Songs(
        artists VARCHAR(200),
        albums VARCHAR(200),
        playedAts VARCHAR(200),
        songNames VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (playedAts)
    )
    """   
connection.execute(sql_query)

# Dump the data
try:
    dataFrame.to_sql("Dharshini_Songs", sqlEngine, index=False, if_exists='append')
except:
    print("Data exists already")

# Close the connection
connection.close()
print("Closed database successfully")
    

            














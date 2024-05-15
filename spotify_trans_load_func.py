import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def extract_data(client, bucket, prefix):
    """Retrieve JSON files from S3 and load them as a list of dictionaries."""
    objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    data_list = []
    keys = []
    
    for obj in objects.get('Contents', []):
        if obj['Key'].endswith('.json'):
            response = client.get_object(Bucket=bucket, Key=obj['Key'])
            data = json.load(response['Body'])
            data_list.append(data)
            keys.append(obj['Key'])
    
    return data_list, keys

def transform_data(data):
    """Transform raw data into dataframes for albums, artists, and songs."""
    albums, artists, songs = [], [], []

    for item in data['items']:
        track = item['track']
        albums.append({
            'album_id': track['album']['id'],
            'name': track['album']['name'],
            'release_date': track['album']['release_date'],
            'total_tracks': track['album']['total_tracks'],
            'url': track['album']['external_urls']['spotify']
        })
        for artist in track['artists']:
            artists.append({
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            })
        songs.append({
            'song_id': track['id'],
            'song_name': track['name'],
            'duration_ms': track['duration_ms'],
            'url': track['external_urls']['spotify'],
            'popularity': track['popularity'],
            'song_added': item['added_at'],
            'album_id': track['album']['id'],
            'artist_id': track['artists'][0]['id']
        })
    
    album_df = pd.DataFrame(albums).drop_duplicates('album_id')
    artist_df = pd.DataFrame(artists).drop_duplicates('artist_id')
    song_df = pd.DataFrame(songs)

    album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    song_df['song_added'] = pd.to_datetime(song_df['song_added'])

    return album_df, artist_df, song_df

def upload_dataframe_to_s3(df, bucket, key_prefix, filename):
    """Upload a dataframe to S3 as a CSV."""
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=f"{key_prefix}/{filename}", Body=buffer.getvalue())
    logger.info(f"Uploaded {filename} to {key_prefix}")

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket_name = "spotify-etl-project-vigsk"
    raw_prefix = "raw/to_be_processed/"
    
    spotify_data, spotify_keys = extract_data(s3_client, bucket_name, raw_prefix)
    
    for data in spotify_data:
        album_df, artist_df, song_df = transform_data(data)
        
        upload_dataframe_to_s3(song_df, bucket_name, "transformed/songs_data", f"songs_transformed_{datetime.now()}.csv")
        upload_dataframe_to_s3(album_df, bucket_name, "transformed/album_data", f"album_transformed_{datetime.now()}.csv")
        upload_dataframe_to_s3(artist_df, bucket_name, "transformed/artists_data", f"artist_transformed_{datetime.now()}.csv")
        
    # Cleanup processed files
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        s3_resource.Object(bucket_name, key).delete()
        logger.info(f"Deleted {key} after processing.")


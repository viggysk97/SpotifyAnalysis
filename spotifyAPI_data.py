import json
import os
import boto3
from datetime import datetime
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_spotify_client():
    """Creates a Spotify client using environment variables for authentication."""
    client_id = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    if not client_id or not client_secret:
        logger.error("Spotify client ID or secret not found.")
        raise EnvironmentError("Spotify client ID or secret not set in environment variables.")
    
    credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return Spotify(client_credentials_manager=credentials_manager)

def get_playlist_data(sp, playlist_uri):
    """Extracts playlist data using the Spotify client and playlist URI."""
    try:
        return sp.playlist_tracks(playlist_uri)
    except Exception as e:
        logger.error(f"Failed to fetch playlist data: {str(e)}")
        raise

def upload_to_s3(data, bucket, key):
    """Uploads data to S3 bucket at the specified key."""
    s3 = boto3.client('s3')
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))
        logger.info(f"Successfully uploaded data to {bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload data to S3: {str(e)}")
        raise

def lambda_handler(event, context):
    """Lambda function handler for extracting, transforming and loading Spotify data to S3."""
    sp = create_spotify_client()
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbLZ52XmnySJg"  # Top 50 Bollywood Trending URL
    playlist_uri = playlist_link.split("/")[-1]

    spotify_data = get_playlist_data(sp, playlist_uri)
    filename = f"spotify_raw_{datetime.now().isoformat()}.json"
    bucket_name = "spotify-etl-project-ameet"
    object_key = f"raw/to_be_processed/{filename}"
    
    upload_to_s3(spotify_data, bucket_name, object_key)


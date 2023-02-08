import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
CLIENT_ID = 'd59a904787044b9ba31069e40243bfb6'
CLIENT_SECRET = 'c6bcfe1cfb694221859e255ce2fd85c0'
sp=spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id="CLIENT_ID",client_secret="CLIENT_SECRET"))

def analyze_playlist(user_id, playlist_id):

# Create empty dataframe
 playlist_features_list = ["artist","album","track_name",  "track_id","danceability","energy","key","loudness","mode", "speechiness","instrumentalness","liveness","valence","tempo", "duration_ms","time_signature"]

 playlist_df = pd.DataFrame(columns = playlist_features_list)

# Loop through every track in the playlist, extract features and append the features to the playlist df

 playlist = sp.user_playlist_tracks('souvikpatra071@gmail.com', playlist_id)["tracks"]["items"]
 for track in playlist:
    # Create empty dict
    playlist_features = {}
    # Get metadata
    playlist_features["artist"] = track["track"]["album"]["artists"][0]["name"]
    playlist_features["album"] = track["track"]["album"]["name"]
    playlist_features["track_name"] = track["track"]["name"]
    playlist_features["track_id"] = track["track"]["id"]
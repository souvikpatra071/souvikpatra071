import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
cid='3e56221b785e472c99ff6da9a7c95a4e'
secret='1b92b4de6dda40fa89201243b9055e47'
client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
playlist_link = "https://open.spotify.com/playlist/37i9dQZF1DWV6eas0o9JXU?si=e40822311a764152"
playlist_URI = playlist_link.split("/")[-1].split("?")[0]
track_uris = [x["track"]["uri"] for x in sp.playlist_tracks(playlist_URI)["items"]]
playlist_URI = playlist_link.split("/")[-1].split("?")[0]
track_uris = [x["track"]["uri"] 
for x in sp.playlist_tracks(playlist_URI)["items"]]
print(sp)
for track in sp.playlist_tracks(playlist_URI)["items"]:
    #URI
    track_uri = track["track"]["uri"]
    print("#URI: ",track_uri)
    #Track name
    track_name = track["track"]["name"]
    print("Track name: ",track_name)
    #Main Artist
    artist_uri = track["track"]["artists"][0]["uri"]
    artist_info = sp.artist(artist_uri)
    print("Main Artist: ",artist_info)
    #Name, popularity, genre
    artist_name = track["track"]["artists"][0]["name"]
    artist_pop = artist_info["popularity"]
    artist_genres = artist_info["genres"]
    print(artist_name)
    print("artist_popularity: ",artist_pop)
    #Album
    album = track["track"]["album"]["name"]
    print("Album: ",album)
    #Popularity of the track
    track_pop = track["track"]["popularity"]
    print("#Popularity of the track: ",track_pop)

df = pd.json_normalize(track)
for i in df:
    print(df)
  

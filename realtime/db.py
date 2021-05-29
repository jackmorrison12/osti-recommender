import os
from pymongo import MongoClient, DESCENDING


def get_playlist(uid, wid):

    db = MongoClient(os.getenv('MONGO_URI')).osti

    playlist = db.playlists.find_one(
        {'user_id': uid, 'workout_id': wid}, sort=[('created_at', DESCENDING)])

    playlist['_id'] = str(playlist['_id'])

    return playlist

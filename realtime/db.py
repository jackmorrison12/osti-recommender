import os
from bson.objectid import ObjectId
from pymongo import MongoClient, DESCENDING, ASCENDING
from collections import defaultdict
import numpy as np


def get_initial_data(uid, wid):

    db = MongoClient(os.getenv('MONGO_URI')).osti

    # Get the initial playlist
    playlist = db.playlists.find_one(
        {'user_id': uid, 'workout_id': wid}, sort=[('created_at', DESCENDING)])

    playlist['_id'] = str(playlist['_id'])

    # Get the top 200 recommendations

    workout = db.workout_types.find_one({'_id': ObjectId(wid)})

    user_recs = db.recommendations.find_one({"user_id": uid})
    workout_recs = user_recs['v5'][workout['name']]

    # Get the track and feature info for the top 200 recommendations
    track_ids = [ObjectId(t['track_id']) for t in workout_recs]
    track_data = list(db.tracks.find({"_id": {"$in": list(track_ids)}}))

    for track in track_data:
        track['_id'] = str(track['_id'])

    # Get the deltas for the top 200 recommendations
    tracks_to_find = [t['track_id'] for t in workout_recs]
    two_same_deltas = list(db.workout_deltas.find(
        {"user_id": uid, "workout_type_id": wid, 'track_id': {"$in": tracks_to_find}}))
    print("Same user & workout:", len(two_same_deltas))

    for delta in two_same_deltas:
        delta['_id'] = str(delta['_id'])

    # tracks_to_find = [t for t in tracks_to_find if t not in [
        # d['track_id'] for d in two_same_deltas]]
    one_same_deltas = list(db.workout_deltas.find({"user_id": uid, "workout_type_id": {
                           "$ne": wid}, 'track_id': {"$in": tracks_to_find}}))
    print("Same user:", len(one_same_deltas))

    for delta in one_same_deltas:
        delta['_id'] = str(delta['_id'])

    one_same_deltas = one_same_deltas + list(db.workout_deltas.find(
        {"user_id": {"$ne": uid}, "workout_type_id": wid, 'track_id': {"$in": tracks_to_find}}))
    print("Same user + same workout:", len(one_same_deltas))

    # tracks_to_find = [t for t in tracks_to_find if t not in [
    # d['track_id'] for d in one_same_deltas]]
    no_same_deltas = list(db.workout_deltas.find({"user_id": {"$ne": uid}, "workout_type_id": {
                          "$ne": wid}, 'track_id': {"$in": tracks_to_find}}))
    print("Different user & workout:", len(no_same_deltas))

    for delta in no_same_deltas:
        delta['_id'] = str(delta['_id'])

    two_same_delta_map = defaultdict(list)
    for d in two_same_deltas:
        two_same_delta_map[d['track_id']].append(d)

    one_same_delta_map = defaultdict(list)
    for d in one_same_deltas:
        one_same_delta_map[d['track_id']].append(d)

    no_same_delta_map = defaultdict(list)
    for d in no_same_deltas:
        no_same_delta_map[d['track_id']].append(d)

    delta_map = defaultdict(dict)

    for rec in workout_recs:
        hrs = np.array([])
        cals = np.array([])
        dists = np.array([])
        for delta in two_same_delta_map[rec['track_id']]:
            if 'delta' in delta['heart_rate']:
                hrs = np.append(hrs, delta['heart_rate']['delta'])
            if 'sum' in delta['calories']:
                cals = np.append(cals, delta['calories']['sum'])
            if 'sum' in delta['distance']:
                dists = np.append(dists, delta['distance']['sum'])

        delta_map[rec['track_id']]['two'] = {}

        if len(hrs) > 0:
            delta_map[rec['track_id']]['two']['heart_rate'] = hrs.mean()
        if len(cals) > 0:
            delta_map[rec['track_id']]['two']['calories'] = cals.mean()
        if len(dists) > 0:
            delta_map[rec['track_id']]['two']['distance'] = dists.mean()

        hrs = np.array([])
        cals = np.array([])
        dists = np.array([])
        for delta in one_same_delta_map[rec['track_id']]:
            if 'delta' in delta['heart_rate']:
                hrs = np.append(hrs, delta['heart_rate']['delta'])
            if 'sum' in delta['calories']:
                cals = np.append(cals, delta['calories']['sum'])
            if 'sum' in delta['distance']:
                dists = np.append(dists, delta['distance']['sum'])

        delta_map[rec['track_id']]['one'] = {}

        if len(hrs) > 0:
            delta_map[rec['track_id']]['one']['heart_rate'] = hrs.mean()
        if len(cals) > 0:
            delta_map[rec['track_id']]['one']['calories'] = cals.mean()
        if len(dists) > 0:
            delta_map[rec['track_id']]['one']['distance'] = dists.mean()

        hrs = np.array([])
        cals = np.array([])
        dists = np.array([])
        for delta in no_same_delta_map[rec['track_id']]:
            if 'delta' in delta['heart_rate']:
                hrs = np.append(hrs, delta['heart_rate']['delta'])
            if 'sum' in delta['calories']:
                cals = np.append(cals, delta['calories']['sum'])
            if 'sum' in delta['distance']:
                dists = np.append(dists, delta['distance']['sum'])

        delta_map[rec['track_id']]['none'] = {}

        if len(hrs) > 0:
            delta_map[rec['track_id']]['none']['heart_rate'] = hrs.mean()
        if len(cals) > 0:
            delta_map[rec['track_id']]['none']['calories'] = cals.mean()
        if len(dists) > 0:
            delta_map[rec['track_id']]['none']['distance'] = dists.mean()

    # Get the user target values

    stats = db.user_workout_stats.find_one(
        {'user_id': uid, 'workout_id': wid}, sort=[('default', ASCENDING)])

    stats['_id'] = str(stats['_id'])

    result = {}
    result['playlist'] = playlist
    result['stats'] = stats
    result['recs'] = workout_recs
    result['track_data'] = track_data
    result['deltas'] = delta_map

    return result

import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from pymongo import MongoClient


def v1():

    print("Downloading data from database...")

    db = MongoClient(os.getenv('MONGO_URI')).osti

    cursor = db.listens.aggregate([
        {"$sort": {"time": 1}},
        {"$group": {"_id": "$user_id", "listens": {
            "$push": "$$ROOT"}, "count": {"$sum": 1}}}
    ])
    listens = list(cursor)

    cursor = db.workouts.aggregate([
        {"$sort": {"start_time": 1}},
        {"$group": {"_id": "$user_id",
                    "workouts": {"$push": {
                        "start_time": "$start_time",
                        "end_time": "$end_time",
                        "activity_type": "$activity_type"}},
                    "count": {"$sum": 1}}}])
    workouts = list(cursor)

    print("Downloaded data from database")
    print("Preparing data...")

    listens2 = {}
    for user in listens:
        listens2[user['_id']] = user["listens"]

    workouts2 = {}
    for user in workouts:
        workouts2[user['_id']] = user["workouts"]

    workout_types = db.workouts.distinct("activity_type")

    songs = db.listens.distinct("song_id")

    users = db.users.distinct("_id")

    workout2idx = {}
    idx2workout = {}
    index = 0
    for w in workout_types:
        workout2idx[w] = index
        idx2workout[index] = w
        index += 1

    user2idx = {}
    idx2user = {}
    index = 0
    for u in users:
        user2idx[str(u)] = index
        idx2user[index] = str(u)
        index += 1

    song2idx = {}
    idx2song = {}
    index = 0
    for s in songs:
        song2idx[str(s)] = index
        idx2song[index] = str(s)
        index += 1

    print("Data prepared")
    print("Calculating utility matrix...")

    utility_matrix = np.zeros((len(users), len(workout_types), len(songs)))

    # Iterate over users
    for user in listens2:
        # get the initial workout time and listen time as cur_workout_start, cur_workout_end, cur_workout_id
        cur_workout = 0
    # iterate over the listens
        for listen in listens2[user]:
            # if listen in workout, add that to utility matrix
            if listen['time'] * 1000 >= workouts2[user][cur_workout]['start_time'] and listen['time'] * 1000 <= workouts2[user][cur_workout]['end_time']:
                utility_matrix[user2idx[listen['user_id']]][workout2idx[workouts2[user]
                                                                        [cur_workout]['activity_type']]][song2idx[str(listen["song_id"])]] += 1

    # if listen after workout, increment workout times to next workout and check again
            elif listen["time"] * 1000 > workouts2[user][cur_workout]["end_time"]:
                cur_workout += 1
                if cur_workout >= len(workouts2[user]):
                    break
                # Recheck if it lies in a new workout
                if listen['time'] * 1000 >= workouts2[user][cur_workout]['start_time'] and listen['time'] * 1000 <= workouts2[user][cur_workout]['end_time']:
                    utility_matrix[user2idx[listen['user_id']]][workout2idx[workouts2[user]
                                                                            [cur_workout]['activity_type']]][song2idx[str(listen["song_id"])]] += 1

    # if listen before workout, pass
    print("Calculated utility matrix")
    print("Saving recommendations to database...")

    for uid, user in enumerate(utility_matrix):
        song_ranking = {}
        for wid, workout in enumerate(user):
            nonzero = np.count_nonzero(workout)
            if (nonzero) != 0:
                # Cap recommendations at 100 songs
                songs_to_get = min(nonzero, 100)
                top_songs = np.argpartition(
                    workout, -1*songs_to_get)[-1*songs_to_get:]

                song_map = {}
                for m in top_songs:
                    song_map[idx2song[m]] = workout[m]

                song_ranking[idx2workout[wid]] = []
                for w in sorted(song_map, key=song_map.get, reverse=True):
                    song_ranking[idx2workout[wid]].append(w)

        # Add recommendations to database for this user
        db.recommendations.update_one({'user_id': idx2user[uid]}, {
            '$set': {"v1": song_ranking}}, upsert=True)

    print("Saved recommendations to database")

    return utility_matrix

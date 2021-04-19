import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from pymongo import MongoClient


def v1():
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

    listens2 = {}
    for user in listens:
        listens2[user['_id']] = user["listens"]

    workouts2 = {}
    for user in workouts:
        workouts2[user['_id']] = user["workouts"]

    workout_types = db.workouts.distinct("activity_type")
    print(workout_types)

    songs = db.listens.distinct("song_id")
    print(songs)

    users = db.users.distinct("_id")
    print(users)

    workout2idx = {}
    idx2workout = {}
    index = 0
    for w in workout_types:
        workout2idx[w] = index
        idx2workout[index] = w
        index += 1
    print(workout2idx)
    print(idx2workout)

    user2idx = {}
    idx2user = {}
    index = 0
    for u in users:
        user2idx[str(u)] = index
        idx2user[index] = str(u)
        index += 1
    print(user2idx)
    print(idx2user)

    song2idx = {}
    idx2song = {}
    index = 0
    for s in songs:
        song2idx[str(s)] = index
        idx2song[index] = str(s)
        index += 1
    print(song2idx)
    print(idx2song)

    # Get overlapping users (have both data)
    # NEED TO CALC THESE SIZES
    utility_matrix = np.zeros((len(workout_types), len(users), len(songs)))

    # Iterate over users
    for user in listens2:
        # get the initial workout time and listen time as cur_workout_start, cur_workout_end, cur_workout_id
        cur_workout = 0
    # iterate over the listens
        for listen in listens2[user]:
            # if listen in workout, add that to utility matrix
            if listen['time'] * 1000 >= workouts2[user][cur_workout]['start_time'] and listen['time'] * 1000 <= workouts2[user][cur_workout]['end_time']:
                utility_matrix[workout2idx[workouts2[user][cur_workout]['activity_type']]
                               ][user2idx[listen['user_id']]][song2idx[str(listen["song_id"])]] += 1
    # if listen after workout, increment workout times to next workout and check again
            elif listen["time"] * 1000 > workouts2[user][cur_workout]["end_time"]:
                cur_workout += 1
                if cur_workout >= len(workouts2[user]):
                    break
                # Recheck if it lies in a new workout
                if listen['time'] * 1000 >= workouts2[user][cur_workout]['start_time'] and listen['time'] * 1000 <= workouts2[user][cur_workout]['end_time']:
                    utility_matrix[workout2idx[workouts2[user][cur_workout]['activity_type']]
                                   ][user2idx[listen['user_id']]][song2idx[str(listen["song_id"])]] += 1
    # if listen before workout, pass

    print(utility_matrix)

    max_indicies = np.argpartition(utility_matrix[17][0], -5)[-5:]

    for m in max_indicies:
        print(idx2song[m], ":", utility_matrix[17][0][m])
    return utility_matrix

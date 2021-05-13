import pandas as pd
import numpy as np
from tqdm import tqdm
import os
from pymongo import MongoClient
import time
from collections import defaultdict
import datetime
from scipy.spatial.distance import cdist
import dateutil.parser as parser
from bson import ObjectId


def get_track_data(df, track_map, rating):
    tids, ratings, artists, albums, danceabilities, energies, keys, loudnesses, modes, speechinesses, acousticnesses, instrumentalnesses, livenesses, valences, tempos, time_signatures, release_dates = [
    ], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], []
    for index, row in df.iterrows():

        if row['tid'] not in track_map:
            print(row['tid'])
        elif "features" in track_map[row['tid']] and "danceability" in track_map[row['tid']]['features']:
            tids.append(row['tid'])
            if rating:
                ratings.append(row['rating'])
            else:
                ratings.append(row['count'])
            artists.append([a['id'] for a in track_map[row['tid']]['artists']])
            albums.append(track_map[row['tid']]['album']['id'])
            danceabilities.append(
                track_map[row['tid']]['features']['danceability'])
            energies.append(track_map[row['tid']]['features']['energy'])
            keys.append(track_map[row['tid']]['features']['key'])
            loudnesses.append(track_map[row['tid']]['features']['loudness'])
            modes.append(track_map[row['tid']]['features']['mode'])
            speechinesses.append(
                track_map[row['tid']]['features']['speechiness'])
            acousticnesses.append(
                track_map[row['tid']]['features']['acousticness'])
            instrumentalnesses.append(
                track_map[row['tid']]['features']['instrumentalness'])
            livenesses.append(track_map[row['tid']]['features']['liveness'])
            valences.append(track_map[row['tid']]['features']['valence'])
            tempos.append(track_map[row['tid']]['features']['tempo'])
            time_signatures.append(
                track_map[row['tid']]['features']['time_signature'])
            release_dates.append(track_map[row['tid']]['release_date'])
    df = pd.DataFrame({"tid": tids, "rating": ratings,
                       "artists": artists, "album": albums,
                       "danceability": danceabilities, "energy": energies, "key": keys,
                       "loudness": loudnesses, "mode": modes, "speechiness": speechinesses,
                       "acousticness": acousticnesses, "instrumentalness": instrumentalnesses,
                       "liveness": livenesses, "valence": valences, "tempo": tempos,
                       "time_signature": time_signatures,
                       "release_date": release_dates
                       },
                      columns=['tid', 'rating',
                               'artists', 'album',
                               'danceability',
                               'energy', 'key', 'loudness', 'mode', 'speechiness',
                               'acousticness', 'instrumentalness', 'liveness', 'valence',
                               'tempo', 'time_signature',
                               'release_date'
                               ])
    return df


def v2():

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
    workout_types_2 = db.cf_playlists.distinct("workout")
    workout_types = workout_types + \
        list(set(workout_types_2) - set(workout_types))

    songs = db.tracks.distinct("_id")

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

    # Get overlapping users (have both data)
    utility_matrix = np.zeros((len(workout_types), len(users), len(songs)))

    # Iterate over users
    for user in listens2:
        # get the initial workout time and listen time as cur_workout_start, cur_workout_end, cur_workout_id
        cur_workout = 0
        # iterate over the listens
        for listen in listens2[user]:
            # if listen in workout
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

    print("Calculated utility matrix")
    print("Getting user and workout songs...")

    user_workout_tracks_map = [{'uid': [], 'tid':[], 'rating':[]}
                               for _ in range(len(utility_matrix))]
    workout_tracks_map = [defaultdict(int) for _ in range(len(utility_matrix))]

    tids = set()
    for wid, workout in enumerate(utility_matrix):
        for uid, user in enumerate(workout):
            # Get max value for user to normalise by
            user_max = max(user)
            # For each track in user, if >0, then do idx2song and add idx2user[uid], idx2song[tid], val / total to df
            for tid, rating in enumerate(user):
                if rating > 0:
                    user_workout_tracks_map[wid]['uid'].append(idx2user[uid])
                    user_workout_tracks_map[wid]['tid'].append(idx2song[tid])
                    user_workout_tracks_map[wid]['rating'].append(
                        (rating)/user_max)
                    workout_tracks_map[wid][idx2song[tid]] += rating
                    tids.add(tid)

    # Adds in playlists of songs

    # cursor = db.cf_playlists.find({})
    # cf_playlists = list(cursor)
    # print(len(tids))

    # for i, playlist in enumerate(cf_playlists):
    #     for track in playlist['tracks']:
    #         # print(track)
    #         workout_tracks_map[workout2idx[playlist['workout']]][track] += 1
    #         # tids.add(str(track))
    #         tids.add(ObjectId(track))

    cursor = db.listens.aggregate([{"$sort": {"time": 1}}, {"$group": {"_id": {"user_id": "$user_id", "song_id": "$song_id"}, "total": {
                                  "$sum": 1}}}, {"$group": {"_id":  "$_id.user_id", "listens": {"$push": {"track_id": "$_id.song_id", "total": "$total"}}}}])
    user_tracks_map = list(cursor)

    for u in user_tracks_map:
        for l in u['listens']:
            tids.add(l['track_id'])

    cursor = db.tracks.find({"_id": {"$in": list(tids)}})
    track_info = list(cursor)

    track_map = {}
    # Make a dictionary of id to features
    for track in track_info:
        track_map[str(track['_id'])] = track

    # Convert all 3 maps to dataframes
    user_workout_tracks = []
    for w in user_workout_tracks_map:
        df = pd.DataFrame(w, columns=['uid', 'tid', 'rating'])
        user_workout_tracks.append(df)

    workout_tracks = []
    for w in workout_tracks_map:
        if len(w.values()) > 0:
            max_listens = max(w.values())
        else:
            max_listens = 1
        df = pd.DataFrame({"tid": w.keys(), "count": [
                          c/(2*max_listens) for c in w.values()]}, columns=['tid', 'count'])
        workout_tracks.append(df)

    user_tracks = [pd.DataFrame({"tid": [], "count":[]}, columns=[
                                'tid', 'count']) for _ in range(len(users))]

    for u in user_tracks_map:
        max_listens = max([l['total'] for l in u['listens']])
        df = pd.DataFrame({"tid": [str(l['track_id']) for l in u['listens']], "count": [
                          l['total']/max_listens for l in u['listens']]}, columns=['tid', 'count'])
        user_tracks[user2idx[u['_id']]] = df

    user_tracks_features = []
    for df in tqdm(user_tracks):
        user_tracks_features.append(get_track_data(df, track_map, False))

    workout_tracks_features = []
    for df in tqdm(workout_tracks):
        workout_tracks_features.append(get_track_data(df, track_map, False))

    print("User and workout songs retrieved")
    print("Calculating recommendations and saving to database...")

    cursor = db.boosts.find({})
    boosts = list(cursor)
    cursor = db.workout_types.find({})
    workout_ids = list(cursor)

    wid2workout = {}
    workout2wid = {}
    for w in workout_ids:
        wid2workout[str(w['_id'])] = w['name']
        workout2wid[w['name']] = str(w['_id'])

    boost_map = defaultdict(lambda: defaultdict(list))
    for boost in boosts:
        boost_map[boost['user_id']][boost['workout_id']].append(
            {'tid': boost['track_id'], 'value': boost['value']})

    # for u in users:
    for user in users:
        user_recommendations_v2 = {}
        user_recommendations_v3 = {}
        user_recommendations_v4 = {}
        user_recommendations_v5 = {}

        # For each workout type of theirs - need a way to check if they have any listens for a particular workout, so iterate
        # over each workout type, and if user_workout_tracks for uid and wid > 0, then generate recommendations
        for wid, workout_type in enumerate(user_workout_tracks):
            if (workout_type[workout_type.uid == str(user)].shape[0] > 0):
                print("Generating", idx2workout[wid], "recommendations for user", str(
                    user), "...")
                # Get user taste from user_workout_tracks - get the features of the needed songs from the db
                workout_listening_history = workout_type[workout_type.uid == str(
                    user)]
                workout_listening_history = get_track_data(
                    workout_listening_history, track_map, True)

                # Get the mean values of all the numeric columns - weighted by rating
                avgs = []
                for col in workout_listening_history:
                    if col not in ['tid', 'rating', 'artists', 'album', 'release_date']:
                        avgs.append(np.average(
                            workout_listening_history[col], weights=workout_listening_history["rating"]))
                    if col == 'release_date':
                        release_date_avg = np.average(workout_listening_history[col].apply(
                            lambda x: parser.parse(x).timestamp()), weights=workout_listening_history["rating"])

                # Make a one-hot encoding of album and artists, but values can be more than one depending on popularity
                artist_map = defaultdict(int)
                for index, row in workout_listening_history.iterrows():
                    if "spotify" in track_map[row['tid']]:
                        for a in row['artists']:
                            artist_map[a] += 1
                artist_max = max(artist_map.values())

                workout_listening_history['rating'] += 0.5

                # Get the trackset of potential tracks
                trackset = pd.concat([workout_listening_history, workout_tracks_features[wid].sort_values('rating', ascending=False).head(
                    200), user_tracks_features[user2idx[str(user)]].sort_values('rating', ascending=False).head(1000)])

                trackset = trackset.sort_values(
                    'rating', ascending=False).drop_duplicates('tid')
                trackset = trackset.reset_index()
                # Calculate cosine distance between the mean value vector and every song in user_tracks union workout_tracks - ADD IN RELEASE DATE
                cosine = cdist([avgs], trackset.drop(
                    ['index', 'rating', 'artists', 'album', 'release_date'], axis=1).iloc[:, 1:], metric='cosine')

                # Calculate the values with just artist weighting, and then also with
                # more weighting on songs they usually listen to
                combined = []
                weighted = []
                avg_rating = trackset['rating'].mean()
                for index, row in trackset.iterrows():
                    total = 2 * cosine[0][index]
                    if "spotify" in track_map[row['tid']]:
                        for a in row['artists']:
                            total += ((artist_map[a]/artist_max) *
                                      (max(cosine[0])/4))/len(row['artists'])
                    combined.append(total)
                    weighted.append(
                        total*(max(1.3, row['rating'] + (avg_rating/3))))

                v2_max_list = np.argsort(cosine[0])[::-1]
                recs = []
                for i, m in enumerate(v2_max_list[:200]):
                    recs.append(
                        {'track_id': trackset.iloc[m]['tid'], 'score': cosine[0][m]})
                user_recommendations_v2[idx2workout[wid]] = recs

                v3_max_list = np.argsort(combined)[::-1]
                recs = []
                for i, m in enumerate(v3_max_list[:200]):
                    recs.append(
                        {'track_id': trackset.iloc[m]['tid'], 'score': combined[m]})
                user_recommendations_v3[idx2workout[wid]] = recs

                v4_max_list = np.argsort(weighted)[::-1]
                recs = []
                for i, m in enumerate(v4_max_list[:200]):
                    recs.append(
                        {'track_id': trackset.iloc[m]['tid'], 'score': weighted[m]})
                user_recommendations_v4[idx2workout[wid]] = recs

                feedback = weighted.copy()
                weight = max(weighted) / 2

                for boost in boost_map[str(user)][workout2wid[idx2workout[wid]]]:
                    feedback[trackset[trackset['tid'] == boost['tid']
                                      ].index.values[0]] += (int(boost['value']) * weight)

                v5_max_list = np.argsort(feedback)[::-1]
                recs = []
                for i, m in enumerate(v5_max_list[:200]):
                    recs.append(
                        {'track_id': trackset.iloc[m]['tid'], 'score': feedback[m]})
                user_recommendations_v5[idx2workout[wid]] = recs

        db.recommendations.update_one({'user_id': str(user)}, {'$set': {
                                      "v2": user_recommendations_v2, "v3": user_recommendations_v3, "v4": user_recommendations_v4, "v5": user_recommendations_v5, "updated_at": int(round(time.time()))}}, upsert=True)

    print("Saved recommendations to database")

    return utility_matrix

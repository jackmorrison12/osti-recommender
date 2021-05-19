import numpy as np
from collections import defaultdict
from pymongo import MongoClient
from scipy.spatial.distance import cdist
import time
import collections
from functools import partial
from bson import ObjectId
import os


def get_track_rankings(trackset, delta_map, song_map, target_deltas, pointer):
    track_rankings = []
    for i, rec in enumerate(trackset):
        hrs = np.array([])
        cals = np.array([])
        steps = np.array([])
        dists = np.array([])
        speeds = np.array([])
        for delta in delta_map[rec['track_id']]:
            if 'delta' in delta['heart_rate']:
                hrs = np.append(hrs, delta['heart_rate']['delta'])
            if 'sum' in delta['calories']:
                cals = np.append(cals, delta['calories']['sum'])
            if 'sum' in delta['steps']:
                steps = np.append(steps, delta['steps']['sum'])
            if 'sum' in delta['distance']:
                dists = np.append(dists, delta['distance']['sum'])
            if 'delta' in delta['speed']:
                speeds = np.append(speeds, delta['speed']['delta'])

        segs_to_check = int(song_map[rec['track_id']]
                            ['features']['duration']/10000)

        targets = []
        avgs = []

        if len(hrs) > 0 and len(target_deltas['heart_rates']) > 0:
            targets.append(target_deltas['heart_rates'][pointer:(
                pointer + segs_to_check)][-1] - target_deltas['heart_rates'][pointer:(pointer + segs_to_check)][0])
            avgs.append(hrs.mean())
        if len(cals) > 0 and len(target_deltas['calories']) > 0:
            c = target_deltas['calories'][pointer:(pointer + segs_to_check)]
            targets.append(sum(c))
            avgs.append(cals.mean())
        if len(steps) > 0 and len(target_deltas['steps']) > 0:
            s = target_deltas['steps'][pointer:(pointer + segs_to_check)]
            targets.append(sum(s))
            avgs.append(steps.mean())
        if len(dists) > 0 and len(target_deltas['distances']) > 0:
            d = target_deltas['distances'][pointer:(pointer + segs_to_check)]
            targets.append(sum(d))
            avgs.append(dists.mean())
        if len(speeds) > 0 and len(target_deltas['speeds']) > 0:
            targets.append(target_deltas['speeds'][pointer:(
                pointer + segs_to_check)][-1] - target_deltas['speeds'][pointer:(pointer + segs_to_check)][0])
            avgs.append(speeds.mean())
        # print(targets)
        # print(avgs)
        cosine = cdist([targets], [avgs], metric='cosine')
        if cosine[0][0] == 0:
            track_rankings.append(1)
        else:
            track_rankings.append(cosine[0][0])

    return track_rankings


def generate_playlist(uid, wid):

    print("Downloading data from database...")

    db = MongoClient(os.getenv('MONGO_URI')).osti

    cursor = db.listens.find({"user_id": uid}).sort("time")
    listens = list(cursor)

    cursor = db.workouts.find(
        {"user_id": uid, "workout_id": wid}).sort("start_time")
    workouts = list(cursor)

    print("Downloaded data from database")
    print("Preparing data...")

    # Append the track ids, in order, to each workout object
    cur_workout = 0

    track_ids = set()

    for w in workouts:
        w['tracks'] = []
    # iterate over the listens
    for listen in listens:
        # if listen in workout
        if listen['time'] * 1000 >= workouts[cur_workout]['start_time'] and listen['time'] * 1000 <= workouts[cur_workout]['end_time']:
            workouts[cur_workout]['tracks'].append(listen)
            track_ids.add(listen['song_id'])
        # if listen after workout, increment workout times to next workout and check again
        elif listen["time"] * 1000 > workouts[cur_workout]["end_time"]:
            cur_workout += 1
            if cur_workout >= len(workouts):
                break
            while listen['time'] * 1000 > workouts[cur_workout]["end_time"]:
                cur_workout += 1
                if cur_workout >= len(workouts):
                    break
            if cur_workout >= len(workouts):
                break
            # Recheck if it lies in a new workout
            if listen['time'] * 1000 >= workouts[cur_workout]['start_time'] and listen['time'] * 1000 <= workouts[cur_workout]['end_time']:
                workouts[cur_workout]['tracks'].append(listen)
                track_ids.add(listen['song_id'])

        # if listen before workout, pass

    songs = db.tracks.find({"_id": {"$in": list(track_ids)}})
    songs = list(songs)

    song_map = {}
    for song in songs:
        song_map[str(song["_id"])] = song

    print("Data prepared")
    print("Calculating user stats...")

    # Save this to database - overridden tag is false
    # Check if there is a record with overridden tag
    # If there is, use those values instead

    user_stats_list = defaultdict(partial(np.ndarray, 0))
    user_stats = {}
    for workout in workouts:
        user_stats_list['average_length'] = np.append(user_stats_list['average_length'], (int(
            workout['end_time']) - int(workout['start_time']))/60000)
        if 'active_minutes' in workout:
            user_stats_list['average_active'] = np.append(
                user_stats_list['average_active'], int(workout['active_minutes']))
        if 'calories_burned' in workout:
            user_stats_list['average_calories'] = np.append(
                user_stats_list['average_calories'], int(workout['calories_burned']))

        if 'heart_rate' in workout:
            user_stats_list['average_heart_rate'] = np.append(
                user_stats_list['average_heart_rate'], workout['heart_rate']['avg'])
            user_stats_list['average_heart_rate_max'] = np.append(
                user_stats_list['average_heart_rate_max'], workout['heart_rate']['max'])
            user_stats_list['average_heart_rate_min'] = np.append(
                user_stats_list['average_heart_rate_min'], workout['heart_rate']['min'])

        # Distance/Speed: Walking, Running, Cycling
        if workout['workout_id'] in ['6091a14a27f7f3b3a9e65134', '6091a67f96e683e8598e6792', '6091a67b96e683e8598e6574']:
            if 'distance' in workout:
                user_stats_list['average_distance'] = np.append(
                    user_stats_list['average_distance'], workout['distance'])
            if 'speed' in workout:
                user_stats_list['average_speed'] = np.append(
                    user_stats_list['average_speed'], workout['speed']['avg'])
                user_stats_list['average_speed_max'] = np.append(
                    user_stats_list['average_speed_max'], workout['speed']['max'])
                user_stats_list['average_speed_min'] = np.append(
                    user_stats_list['average_speed_min'], workout['speed']['min'])

            # Steps: Walking, Running
            if workout['workout_id'] in ['6091a14a27f7f3b3a9e65134', '6091a67f96e683e8598e6792']:
                if "steps" in workout:
                    user_stats_list['average_steps'] = np.append(
                        user_stats_list['average_steps'], workout['steps'])

    if len(user_stats_list['average_length']) > 0:
        user_stats['average_length'] = user_stats_list['average_length'].mean()
    if len(user_stats_list['average_active']) > 0:
        user_stats['average_active'] = user_stats_list['average_active'].mean()
    if len(user_stats_list['average_distance']) > 0:
        user_stats['average_distance'] = user_stats_list['average_distance'].mean()
    if len(user_stats_list['average_speed']) > 0:
        user_stats['average_speed'] = user_stats_list['average_speed'].mean()
    if len(user_stats_list['average_speed_max']) > 0:
        user_stats['average_speed_max'] = user_stats_list['average_speed_max'].mean()
    if len(user_stats_list['average_speed_min']) > 0:
        user_stats['average_speed_min'] = user_stats_list['average_speed_min'].mean()
    if len(user_stats_list['average_steps']) > 0:
        user_stats['average_steps'] = user_stats_list['average_steps'].mean()
    if len(user_stats_list['average_calories']) > 0:
        user_stats['average_calories'] = user_stats_list['average_calories'].mean()
    if len(user_stats_list['average_heart_rate']) > 0:
        user_stats['average_heart_rate'] = user_stats_list['average_heart_rate'].mean()
    if len(user_stats_list['average_heart_rate_max']) > 0:
        user_stats['average_heart_rate_max'] = user_stats_list['average_heart_rate_max'].mean()
    if len(user_stats_list['average_heart_rate_min']) > 0:
        user_stats['average_heart_rate_min'] = user_stats_list['average_heart_rate_min'].mean()
    if 'average_active' in user_stats and 'average_length' in user_stats:
        user_stats['active_percentage'] = user_stats['average_active'] / \
            user_stats['average_length']

    db.user_workout_stats.update_one({'user_id': uid, 'workout_id': wid, 'default': True}, {
                                     "$set": {"stats": user_stats, "updated_at": int(round(time.time()))}}, upsert=True)

    override = db.user_workout_stats.find_one(
        {'user_id': uid, 'workout_id': wid, 'default': False})

    if override:
        user_stats = override['stats']

    print("Calculated user stats")

    print("Calculating workout plots...")

    heart_rates = defaultdict(list)
    calories = defaultdict(list)
    distances = defaultdict(list)
    speeds = defaultdict(list)
    steps = defaultdict(list)

    for workout in workouts:
        for dp in workout['data']:
            if "heart_rate" in dp:
                heart_rates[(
                    int(dp['start_time']) - int(workout['start_time']))/1000].append(dp['heart_rate'])
            if "calories_burned" in dp:
                calories[(int(dp['start_time']) - int(workout['start_time'])
                          )/1000].append(dp['calories_burned'])
            if workout['workout_id'] in ['6091a14a27f7f3b3a9e65134', '6091a67f96e683e8598e6792', '6091a67b96e683e8598e6574']:
                if 'distance' in dp:
                    distances[(
                        int(dp['start_time']) - int(workout['start_time']))/1000].append(dp['distance'])
                if 'speed' in dp:
                    speeds[(int(dp['start_time']) -
                            int(workout['start_time']))/1000].append(dp['speed'])
                if workout['workout_id'] in ['6091a14a27f7f3b3a9e65134', '6091a67f96e683e8598e6792']:
                    if 'steps' in dp:
                        steps[(int(dp['start_time']) -
                               int(workout['start_time']))/1000].append(dp['steps'])

    workout_plots = {}
    if len(heart_rates) > 0:
        workout_plots['heart_rates'] = collections.OrderedDict(
            sorted(heart_rates.items()))
    if len(calories) > 0:
        workout_plots['calories'] = collections.OrderedDict(
            sorted(calories.items()))
    if len(distances) > 0:
        workout_plots['distances'] = collections.OrderedDict(
            sorted(distances.items()))
    if len(speeds) > 0:
        workout_plots['speeds'] = collections.OrderedDict(
            sorted(speeds.items()))
    if len(steps) > 0:
        workout_plots['steps'] = collections.OrderedDict(sorted(steps.items()))

    for w in workout_plots:
        xpoints = np.array([])
        ypoints = np.array([])
        for c in workout_plots[w]:
            if len(workout_plots[w][c]) > min(2, len(workouts)/3):
                xpoints = np.append(xpoints, c)
                ypoints = np.append(ypoints, np.mean(workout_plots[w][c]))
        workout_plots[w] = [xpoints, ypoints]

    print("Calculated workout plots")

    print("Calculating targets...")

    # Get the subset of this which we're gonna make a playlist for, usually the average length
    # (or if a user has changed this, use that)

    length = user_stats['average_length']
    num_datapoints = int(6*length)

    for w in workout_plots:
        last_dp = 0
        for index in workout_plots[w][0]:
            if index < num_datapoints*10:
                last_dp += 1
            else:
                break
        workout_plots[w][0] = workout_plots[w][0][:last_dp]
        workout_plots[w][1] = workout_plots[w][1][:last_dp]

    # Get the values we want to hold each value at (as vectors, 10 intervals, as in future
    # these may change over time)
    targets = {}
    multiplier = 1.05
    if 'average_calories' in user_stats:
        targets['calories'] = [
            (user_stats['average_calories']/num_datapoints) * multiplier] * num_datapoints
    if 'average_heart_rate' in user_stats:
        targets['heart_rates'] = [
            user_stats['average_heart_rate']] * num_datapoints
    if 'average_distance' in user_stats:
        targets['distances'] = [
            (user_stats['average_distance']/num_datapoints) * multiplier] * num_datapoints
    if 'average_speed' in user_stats:
        targets['speeds'] = [(user_stats['average_speed'])
                             * multiplier] * num_datapoints
    if 'average_steps' in user_stats:
        targets['steps'] = [
            (user_stats['average_steps']/num_datapoints) * multiplier] * num_datapoints

    # Calculate what we need to do at 10s interval to keep the value at the target

    # Iterate over each value in workout_plots
    # Iterate through each of those 10s increments, find the corresponding one in the targets,
    # and work out the delta needed
    # Keep a record of needed deltas to target

    multipliers = {"heart_rates": 1,
                   "calories": 2,
                   "distances": 2,
                   "speeds": 1,
                   "steps": 2}

    target_deltas = defaultdict(list)

    for w in workout_plots:
        if len(workout_plots[w][0]) != 0:
            for i in range(num_datapoints):
                if (i * 10) in workout_plots[w][0]:
                    target_deltas[w].append((multipliers[w] * targets[w][i]) - workout_plots[w]
                                            [1][np.where(workout_plots[w][0] == (i * 10))[0][0]])
                else:
                    if multipliers[w] == 1:
                        target_deltas[w].append(0)
                    else:
                        target_deltas[w].append(targets[w][i])

    print("Calculated targets")

    print("Calculating song deltas...")

    # We only need to do this for the workouts which haven't already had
    # their deltas calculated

    last_workout = list(db.workout_deltas.find(
        {"user_id": uid, "workout_type_id": wid}).sort('time', -1).limit(1))
    start_time = last_workout[0]['time'] if len(last_workout) > 0 else '0'

    deltas = []
    for workout in workouts:
        if workout['start_time'] > start_time:

            data = workout['data']
            data_len = len(data)
            data_pointer = 1
            for i, track in enumerate(workout['tracks']):
                data_pointer -= 1
                start = int(track['time'])*1000
                if i < len(workout['tracks']) - 1:
                    end = int(workout['tracks'][i+1]['time'])*1000
                else:
                    end = int(int(workout['end_time']))

                hrs = np.array([])
                calories = np.array([])
                steps = np.array([])
                distances = np.array([])
                speeds = np.array([])
                while(data_pointer < data_len and int(data[data_pointer]['start_time']) < end):
                    # print(data[data_pointer])
                    if 'heart_rate' in data[data_pointer]:
                        hrs = np.append(hrs, data[data_pointer]['heart_rate'])
                    if 'calories_burned' in data[data_pointer]:
                        calories = np.append(
                            calories, data[data_pointer]['calories_burned'])
                    if 'steps' in data[data_pointer]:
                        steps = np.append(steps, data[data_pointer]['steps'])
                    if 'distance' in data[data_pointer]:
                        distances = np.append(
                            distances, data[data_pointer]['distance'])
                    if 'speed' in data[data_pointer]:
                        speeds = np.append(speeds, data[data_pointer]['speed'])
                    data_pointer += 1

                arr = speeds

                heart_rate = {"avg": hrs.mean(), "min": hrs.min(), "max": hrs.max(
                ), "delta": hrs[-1] - hrs[0]} if len(hrs) > 0 else {}
                calories = {"sum": calories.sum(), "avg": calories.mean(), "min": calories.min(
                ), "max": calories.max(), "delta": calories[-1] - calories[0]} if len(calories) > 0 else {}
                steps = {"sum": steps.sum(), "avg": steps.mean(), "min": steps.min(
                ), "max": steps.max(), "delta": steps[-1] - steps[0]} if len(steps) > 0 else {}
                distance = {"sum": distances.sum(), "avg": distances.mean(), "min": distances.min(
                ), "max": distances.max(), "delta": distances[-1] - distances[0]} if len(distances) > 0 else {}
                speed = {"avg": speeds.mean(), "min": speeds.min(), "max": speeds.max(
                ), "delta": speeds[-1] - speeds[0]} if len(speeds) > 0 else {}

                deltas.append({"track_id": str(song_map[str(track['song_id'])]['_id']),
                               "user_id": uid,
                               "workout_type_id": wid,
                               "workout_id": str(workout['_id']),
                               "time": workout['end_time'],
                               "heart_rate": heart_rate,
                               "calories": calories,
                               "steps": steps,
                               "distance": distance,
                               "speed": speed
                               })
    if len(deltas) > 0:
        db.workout_deltas.insert_many(deltas)

    print("Calculated song deltas")

    print("Generating playlist...")

    user_recs = db.recommendations.find_one({"user_id": uid})
    workout_recs = user_recs['v5'][workouts[0]['activity_type']]

    track_ids = [ObjectId(t['track_id']) for t in workout_recs]

    # Update the song map to add any recommended songs not already in there
    songs = db.tracks.find({"_id": {"$in": list(track_ids)}})
    songs = list(songs)

    song_map = {}
    for song in songs:
        song_map[str(song["_id"])] = song

    workout_recs = user_recs['v5'][workouts[0]['activity_type']]

    tracks_to_find = [t['track_id'] for t in workout_recs]
    two_same_deltas = list(db.workout_deltas.find(
        {"user_id": uid, "workout_type_id": wid, 'track_id': {"$in": tracks_to_find}}))
    print("Same user & workout:", len(two_same_deltas))

    tracks_to_find = [t for t in tracks_to_find if t not in [
        d['track_id'] for d in two_same_deltas]]
    one_same_deltas = list(db.workout_deltas.find({"user_id": uid, "workout_type_id": {
                           "$ne": wid}, 'track_id': {"$in": tracks_to_find}}))
    print("Same user:", len(one_same_deltas))

    one_same_deltas = one_same_deltas + list(db.workout_deltas.find(
        {"user_id": {"$ne": uid}, "workout_type_id": wid, 'track_id': {"$in": tracks_to_find}}))
    print("Same user + same workout:", len(one_same_deltas))

    tracks_to_find = [t for t in tracks_to_find if t not in [
        d['track_id'] for d in one_same_deltas]]
    no_same_deltas = list(db.workout_deltas.find({"user_id": {"$ne": uid}, "workout_type_id": {
                          "$ne": wid}, 'track_id': {"$in": tracks_to_find}}))
    print("Different user & workout:", len(no_same_deltas))

    two_same_delta_map = defaultdict(list)
    for d in two_same_deltas:
        two_same_delta_map[d['track_id']].append(d)

    one_same_delta_map = defaultdict(list)
    for d in one_same_deltas:
        one_same_delta_map[d['track_id']].append(d)

    no_same_delta_map = defaultdict(list)
    for d in no_same_deltas:
        no_same_delta_map[d['track_id']].append(d)

    # Set a pointer starting at 0 - this will track where in the workout we've predicted
    # songs til
    # While this is less than the length of the workout, keep looking for songs
    # Add these track ids to a list
    # On each iteration, go over each song in the trackset
    # Assign a score to each track on how well it lines up
    # The one with the highest score is added to the list and removed from the trackset

    # To use:
    # - target_deltas - these are what we need to do for each value every 10s
    # - xxx_same_delta_map - these are the deltas each track provides, indexed on trackid
    # - workout_rec_map - these are the recommended tracks, in order of best to worst

    trackset = workout_recs.copy()

    # Number of seconds into the workout
    pointer = 0
    playlist = []
    while (pointer < num_datapoints):
        # Look for songs - give each a ranking
        track_rankings = []
        two_track_rankings = np.array(get_track_rankings(
            trackset, two_same_delta_map, song_map, target_deltas, pointer))
        # two_track_rankings = [r if not np.isnan(r) else np.nanmean(two_track_rankings) for r in two_track_rankings]
        one_track_rankings = np.array(get_track_rankings(
            trackset, one_same_delta_map, song_map, target_deltas, pointer))
        # one_track_rankings = [r if not np.isnan(r) else np.nanmean(one_track_rankings) for r in one_track_rankings]
        no_track_rankings = np.array(get_track_rankings(
            trackset, no_same_delta_map, song_map, target_deltas, pointer))
        # no_track_rankings = [r if not np.isnan(r) else np.nanmean(no_track_rankings) for r in no_track_rankings]

        # print(no_track_rankings)

        for i in range(len(trackset)):
            two = two_track_rankings[i] if not np.isnan(
                two_track_rankings[i]) else 1
            one = one_track_rankings[i] if not np.isnan(
                one_track_rankings[i]) else 1
            zero = no_track_rankings[i] if not np.isnan(
                no_track_rankings[i]) else 1

            # Do the bpm vs heart rate
            tempo = song_map[trackset[i]['track_id']]['features']['tempo']
            # target_hr =
            segs_to_check = int(
                song_map[trackset[i]['track_id']]['features']['duration']/10000)
            target_hr = np.add(np.array(targets['heart_rates'][pointer:(pointer + segs_to_check)]), np.array(
                target_deltas['heart_rates'][pointer:(pointer + segs_to_check)])).mean()
            bpm_dist = abs((tempo - target_hr)/target_hr)

            # Check position in the recommendations

            track_rankings.append(0.5 * two + 0.2 * one +
                                  0.1 * zero + 0.3 * bpm_dist + (i/100000))

        # print(track_rankings)

        top_track = track_rankings.index(min(track_rankings))
        playlist.append(trackset[top_track]['track_id'])
        # print(song_map[trackset[top_track]['track_id']]['name'])
        # print(track_rankings[top_track])
        # print(target_hr)
        pointer += int(song_map[trackset[top_track]
                       ['track_id']]['features']['duration']/10000)
        del trackset[top_track]
    print("Playlist created:")
    for song in playlist:
        print(song_map[song]['name'])
    print(pointer/6, "mins")
    print(len(playlist), "songs")

    print("Generated playlists")

    print("Saving playlist to database...")

    db.playlists.insert_one({'user_id': uid, 'workout_id': wid,
                            "tracks": playlist, "created_at": int(round(time.time()))})

    # If more than 10 playlists for this user and workout, delete the oldest one
    playlists = list(db.playlists.find(
        {'user_id': uid, 'workout_id': wid}).sort("created_at"))
    if len(playlists) > 10:
        db.playlists.delete_one({"_id": playlists[0]['_id']})

    print("Playlist saved to database")
    return 1

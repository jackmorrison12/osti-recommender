from flask import Flask, request
from flask_mongoengine import MongoEngine
import os
from pathlib import Path
from dotenv import load_dotenv
from recommendation_engines.v1 import v1
from recommendation_engines.v2 import v2
from playlist_engines.v1 import generate_playlist
from playlist_engines.v1 import generate_all_playlists
from realtime.db import get_initial_data

from celery import Celery

# Get the base directory
basepath = Path()
basedir = str(basepath.cwd())
# Load the environment variables
envars = basepath.cwd() / '.env'
load_dotenv(envars)

app = Flask(__name__)

app.config['CELERY_BROKER_URL'] = os.getenv('REDIS_URL')
app.config['CELERY_RESULT_BACKEND'] = os.getenv('REDIS_URL')

db = MongoEngine()

app.config['MONGODB_SETTINGS'] = {
    'db': os.getenv('MONGO_DB'),
    'host':  os.getenv('MONGO_URI')
}

db.init_app(app)

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


@celery.task
def v1_redis():
    with app.app_context():
        v1()
        return "Running v1"


@celery.task
def v2_redis():
    with app.app_context():
        v2()
        return "Running v2"


@celery.task
def playlist_redis(uid, wid):
    with app.app_context():
        generate_playlist(uid, wid)
        return "Generating playlist for user " + uid + " workout " + wid


@celery.task
def playlist_all_redis():
    with app.app_context():
        generate_all_playlists()
        return "Generating all playlists"


@ app.route('/v1')
def v1_flask():
    print("Running v1...")
    task = v1_redis.delay()
    print(task)
    return 'v1 triggered'


@ app.route('/v2')
def v2_flask():
    print("Running v2...")
    task = v2_redis.delay()
    print(task)
    return 'v2 triggered'


@ app.route('/generate_playlist', methods=['POST'])
def playlist_flask():
    request_data = request.get_json()
    print("Generating playlist...")
    task = playlist_redis.delay(
        request_data['uid'], request_data['wid'])
    print(task)
    return 'Playlist generation triggered'


@ app.route('/generate_all_playlists', )
def playlist_all_flask():
    print("Generating playlists...")
    task = playlist_all_redis.delay()
    print(task)
    return 'Playlist generations triggered'


@ app.route('/')
def index():
    return "Osti Recommender is Alive"

# Routes for the real time app
@ app.route('/get_initial_data', methods=['POST'])
def get_initial_data_route():
    request_data = request.get_json()
    print("Getting initial data for workout of type",
          request_data['wid'], "for user", request_data['uid'])
    return get_initial_data(request_data['uid'], request_data['wid'])

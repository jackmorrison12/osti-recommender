from flask import Flask, request
from flask_mail import Mail, Message
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

app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USERNAME'] = os.getenv('EMAIL')
app.config['MAIL_PASSWORD'] = os.getenv('EMAIL_PW')
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
app.config['CELERY_BROKER_URL'] = os.getenv('REDIS_URL')
app.config['CELERY_RESULT_BACKEND'] = os.getenv('REDIS_URL')

mail = Mail(app)
db = MongoEngine()

app.config['MONGODB_SETTINGS'] = {
    'db': os.getenv('MONGO_DB'),
    'host':  os.getenv('MONGO_URI')
}

db.init_app(app)

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)


@celery.task
def redis_test(arg1, arg2):
    with app.app_context():
        # some long running task here
        msg = Message('Hello', sender=os.getenv('EMAIL'),
                      recipients=[os.getenv('EMAIL')])
        msg.body = "Sent from redis on Heroku"
        mail.send(msg)
        return "done"


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


@ app.route('/redis')
def redis():
    print("here")
    task = redis_test.delay(10, 20)
    print(task)
    return 'done'


@ app.route('/')
def index():
    # v1()
    # v2()
    # generate_playlist('606c78c40326f734f14f326b', '6091a67b96e683e8598e6525')
    # generate_all_playlists()
    return "Osti Recommender"

# Routes for the real time app


@ app.route('/get_initial_data', methods=['POST'])
def get_initial_data_route():
    request_data = request.get_json()
    print("Getting initial data for workout of type",
          request_data['wid'], "for user", request_data['uid'])
    return get_initial_data(request_data['uid'], request_data['wid'])

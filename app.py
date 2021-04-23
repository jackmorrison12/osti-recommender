from flask import Flask
from flask_mail import Mail, Message
from flask_mongoengine import MongoEngine
import os
from pathlib import Path
from dotenv import load_dotenv
from engines.v1.v1 import v1
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


@ app.route('/v1')
def v1_flask():
    print("Running v1...")
    task = v1_redis.delay()
    print(task)
    return 'v1 triggered'


@ app.route('/redis')
def redis():
    print("here")
    task = redis_test.delay(10, 20)
    print(task)
    return 'done'


@ app.route('/')
def index():
    return "Osti Recommender"

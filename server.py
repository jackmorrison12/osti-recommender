from flask import Flask
from flask import redirect, url_for, request, jsonify
from flask_mail import Mail, Message
from flask_mongoengine import MongoEngine
import datetime

import os
from pathlib import Path
from dotenv import load_dotenv

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

mail = Mail(app)

db = MongoEngine()

app.config['MONGODB_SETTINGS'] = {
    'db': os.getenv('MONGO_DB'),
    'host':  os.getenv('MONGO_URI')
}

db.init_app(app)


class Users(db.DynamicDocument):
    name = db.StringField(required=True)
    email = db.StringField(required=True)
    createdAt = db.DateTimeField(
        default=datetime.datetime.utcnow, required=True)


@ app.route('/db', methods=['GET'])
def query_records():
    email = request.args.get('email')
    user = Users.objects(email=email).first()
    if not user:
        return jsonify({'error': 'data not found'})
    else:
        return jsonify(user)


@ app.route('/db', methods=['POST'])
def create_record():
    # return request.form
    record = request.form
    # return record['name']
    user = Users(name=record['name'],
                 email=record['email'])
    user.save()
    return jsonify(user)


@ app.route("/mail")
def send():
    msg = Message('Hello', sender=os.getenv('EMAIL'),
                  recipients=[os.getenv('EMAIL')])
    msg.body = "This is the email body"
    mail.send(msg)
    return "Sent"


@ app.route('/')
def index():
    return jsonify({'name': 'alice',
                    'email': 'test@outlook.com'})


@ app.route('/hello')
def hello_world():
    return "Hello world"


@ app.route('/product/<name>')
def get_product(name):
    return "The product is " + str(name)


@ app.route('/create/<first_name>/<last_name>')
def create(first_name=None, last_name=None):
    return 'Hello ' + first_name + ',' + last_name


@ app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        user = request.form['name']
        return redirect(url_for('hello_world'))
    else:
        user = request.args.get('name')
        return "hello"


app.run(host='0.0.0.0', port=81, debug=True)

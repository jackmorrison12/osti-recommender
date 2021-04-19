from flask_mongoengine import MongoEngine
import datetime


class Users(MongoEngine().DynamicDocument):
    name = MongoEngine().StringField(required=True)
    email = MongoEngine().StringField(required=True)
    createdAt = MongoEngine().DateTimeField(
        default=datetime.datetime.utcnow, required=True)

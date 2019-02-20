from app import db as database
from sqlalchemy.ext.declarative import DeclarativeMeta
import json
import decimal


class AlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata' and x != 'query' and x!= 'query_class']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data)
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            return fields
        return json.JSONEncoder.default(self, obj)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


class Users(database.Model):
    id = database.Column(database.Integer, primary_key=True)
    first_name = database.Column(database.String(255))
    last_name = database.Column(database.String(255))
    birth_date = database.Column(database.Integer)
    gender = database.Column(database.String(1))
    email = database.Column(database.String(255))


class Locations(database.Model):
    id = database.Column(database.Integer, primary_key=True)
    place = database.Column(database.String(255))
    city = database.Column(database.String(255))
    distance = database.Column(database.Integer)
    country = database.Column(database.String(255))


class Visits(database.Model):
    id = database.Column(database.Integer, primary_key=True)
    user = database.Column(database.Integer, name='user_id')
    location = database.Column(database.Integer, name='location_id')
    mark = database.Column(database.SmallInteger)
    visited_at = database.Column(database.Integer)


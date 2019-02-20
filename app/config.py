import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    POSTGRES = {
        'user': 'postgres',
        'pw': 'qwerty',
        'db': 'tank1',
        'host': 'localhost',
        'port': '5432',
    }
    SQLALCHEMY_DATABASE_URI = 'postgresql://%(user)s:%(pw)s@%(host)s:%(port)s/%(db)s' % POSTGRES
    SQLALCHEMY_TRACK_MODIFICATIONS = False

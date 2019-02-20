from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from app.config import Config

fapp = Flask(__name__)

fapp.config.from_object(Config)

db = SQLAlchemy(fapp)


from app import routes, models

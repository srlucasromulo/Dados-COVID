import pymongo
from pymongo import MongoClient

client = MongoClient()
db = client.covid
last_update = db.update
collection = db.data


import pymongo
from pymongo import MongoClient


class DB:

    __instance = None

    def __init__(self):
        self.client = MongoClient()
        self.database = self.client.covid
        self.last_update = self.database.last_update
        self.country = self.database.country
        self.state = self.database.state
        self.city = self.database.city

    @classmethod
    def singleton(cls):
        if cls.__instance is None:
            cls.__instance = cls()
        return cls.__instance

    def create(self, table, dict):
        command = f'self.{table}.insert_one({dict})'
        # print(command)  #DBG
        exec(command)

    def update(self, table, dict):

        if table == 'last_update':
            query = {'update': dict['last_update']}

        command = f'self.{table}.find_one_and_replace({query},{dict})'
        exec(command)

    def read(self, table, dict):
        command = f'self.{table}.find({dict})'
        exec(command)

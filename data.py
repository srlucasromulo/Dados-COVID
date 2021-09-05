from DB import DB
from datetime import date
import pandas as pd
import numpy as np
import os

import time #dbg

DIR_PREFIX = 'HIST_PAINEL_COVIDBR'
db = DB.singleton()
today = date.today()
first_update = date(2020, 1, 1)
DEBUG = True


# gets a date object from a str date (YYYY-MM-AA)
def get_date(last_update):
    values = last_update.split('-')
    values = [int(x) for x in values]
    return date(values[0], values[1], values[2])


# creates/updates the DB using the downloaded data
def update_DB():

    # gets downloaded dir and filenames
    dit_list = os.listdir()
    for d in dit_list:
        if DIR_PREFIX in d:
            directory = d + '/'
    filenames = sorted(os.listdir(directory))

    # checks if the DB already exists
    #   - if the DB exists, only the last file (last added data)
    #       is necessary
    #   - else saves the date of the DB creation as 2020-01-01
    if db.last_update.count_documents({}):
        filenames = [filenames.pop()]
    else:
        db.last_update.insert_one({
            'last_update': first_update.__format__('%Y-%m-%d'),
            'update': first_update.__format__('%Y-%m-%d')
        })

    # gets the DB last update date
    last_update = db.last_update.find_one()
    last_update = get_date(last_update['update'])


    # opens the files as pandas.DataFrames;
    # goes through the data checking if it is already in the DB (if update date < today),
    # classifies the data in city, state or country and saves to DB
    for filename in filenames:
        if DEBUG:
            print(f'{filename}\nlast_update:{last_update}, today:{today}')  #DBG

        df = pd.read_csv(directory+filename, sep=';').fillna('').iloc

        country = 0
        state = 0
        city = 0

        for row in df:

            dict = row.to_dict()
            dict_date = get_date(dict['data'])

            if today >= dict_date > last_update:

                if dict['municipio'] != '':
                    table = 'city'
                    city += 1

                elif dict['estado'] != '' and dict['codmun'] == '':
                    table = 'state'
                    state += 1

                elif dict['estado'] == '':
                    table = 'country'
                    country += 1

                db.create(table, dict)

        print(country, state, city)

    # updates DB update date as today
    update_date = {
        'last_update': last_update.__format__('%Y-%m-%d'),
        'update': today.__format__('%Y-%m-%d')
    }
    db.update('last_update', update_date)


start = time.time()
update_DB()
finish = time.time()
print(f'tempo decorrido: {finish-start}')

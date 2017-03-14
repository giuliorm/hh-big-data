__author__ = 'JuriaSan'

import json

import requests

from repository import MongoConnection

def load_specializations():
    r = requests.get("https://api.hh.ru/specializations").json()
    return r

def load_cv(cv_id):
    return requests.get("https://api.hh.ru/resumes/" + str(cv_id)).json()

def load_specs(connection):

    if (connection != None):
        specs =  load_specializations()
        for spec in specs:
            connection.add_or_update('specializations', spec, spec)
        return specs
    return None


conn = MongoConnection("127.0.0.1", 27017)
conn.connect('hh-crawler')
specs = conn.find_all('specializations')
specs_len = len(specs)



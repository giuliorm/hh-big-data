from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
class MongoConnection:

    client = None
    host = None
    port = None
    database = None

    def __init__(self, host="127.0.0.1", port=27017):
        self.host = host
        self.port = port


    def connect(self, database_name):
        if (self.client == None):
            try:
                self.client = MongoClient(host=self.host, port=self.port)
                self.database = self.client[database_name]
            except ConnectionFailure:
                print(format("Cannot connect to host {0} port {1}\n", self.host, self.port))
                return False
        return True

    def close(self):
        if self.client != None:
            self.client.close()


    def __check_parameter(self, parameter):
        return parameter != None or parameter != ""

    def find_all(self, collection):
        if self.__check_parameter(collection):
            return self.database[collection].find()

    def add_or_update(self, collection, filter, newDocument):
        if self.__check_parameter(collection):
            col = self.database[collection]
            #find_result = col.find_one(document)
            #if find_result != None:
            col.update_one(filter=filter, update={ "$set": newDocument }, upsert=True)
            #else:
               # col.insert_one(document=document)
            return True
        return False



    def get(self, collection, search_object):
        result = None
        if self.__check_parameter(collection):
            result = self.database[collection].find_one(search_object)
        return result

    def delete(self, collection, document):
        result = None
        if self.__check_parameter(collection):
            result = self.database[collection].delete_many(document)
        return result.deleted_count


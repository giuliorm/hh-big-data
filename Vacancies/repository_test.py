from .repository import MongoConnection

connection = None

def  repo_test_connection():
    c = MongoConnection(host="127.0.0.1", port=27017)
    if c.connect('test'):
       c.close()
       return True
    return False

print("repo_test_connection: Passed" if repo_test_connection() else "repo_test_connection: Failed ")

connection = MongoConnection("127.0.0.1", 27017)
connection.connect('test')

o = {'field1': 1, 'field2': 2}

connection.delete('test', {})

def repo_test_add(o):
    return connection.add_or_update('test', o, o)

print ("repo_test_add: Passed" if repo_test_add(o)  else "repo_test_add: Failed")

def repo_test_duplicates(o):
    return connection.add_or_update('test', o, o)

print ("repo_test_duplicates: Passed" if  repo_test_duplicates(o)  else "repo_test_duplicates: Failed")

def repo_test_filter(filter, o):
    return connection.add_or_update('test', filter, o)

repo_test_add({'fif': 1})
repo_test_filter({ 'field1': 1 }, {'field1':1, 'field3': 3})


connection.close()
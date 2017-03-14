__author__ = 'JuriaSan'
from pymystem3 import Mystem
from repository import MongoConnection
from TextDataHandlers import TextHandler
from pymorphy2 import MorphAnalyzer
from lxml.html import document_fromstring
from scipy import spatial
from nltk.stem.porter import PorterStemmer
from nltk.corpus import stopwords
import enchant
import string

def handle_tokens(words):
    words = [i for i in words if ( i not in string.punctuation )]
    stop_words = stopwords.words('russian')
    stop_words.extend(['что', 'это', 'так', 'вот', 'быть', 'как', 'в', '—', 'к', 'на'])
    words = [remove_punct(i) for i in words if (i not in stop_words )]
    return words

def remove_punct(str):
    return str\
        .replace(',','')\
        .replace('.','')\
        .replace('(','')\
        .replace(")",'')\
        .replace('?','')\
        .replace('!','')\
        .replace(':','')\
        .replace(';','')


def isEnglish(s):
    if s is None or s == '':
        return False
    d = enchant.Dict("en_US")
    return d.check(s)

def handle_english(text):
    if text is None or text == "":
        return text

    words = text.split(' ')
    st = PorterStemmer()
    txt = [st.stem(remove_punct(word)) for word in words if word not in stopwords.words('english')]
    return txt

def get_words(text):
    if text is None or text == "":
        return []
    words = text.split(' ')

    if len(words) > 0:
        if (isEnglish(words[0])):
            return handle_english(text)


def get_text(keys, text):
    if text is None:
        return None
    el = document_fromstring(text)

    bags = []
    prevP = None

    for node in el.xpath("//ul || /p"):
        if prevP != None and prevP.tag == "p" and node.tag == "ul":
            key = "".join([x for x in prevP.itertext()])
            key = remove_punct(key).lower()

def parse_description(text, handl, m):
    if text is None:
        return None
    el = document_fromstring(text)

    bags = []
    prevP = None

    for node in el.xpath("//ul || /p"):
        if prevP != None and prevP.tag == "p" and node.tag == "ul":
            key = "".join([x for x in prevP.itertext()])
            key = remove_punct(key).lower()

            if key == 'требования' or key == 'requirements':
                for li in node:
                    if li.text is not None and li.text != "":
                        txt = None
                        words = li.text.split(' ')
                        if isEnglish(words[0]):
                            txt = handle_english(li.text)
                        else:
                            txt = handl.handle(m.lemmatize(li.text))
                        bags.extend(txt)

            """text = prevP.text
            if text is None:
                try:
                    text = prevP.xpath("//em")[0].text
                except AttributeError:
                    pass """
        #    if text != None:
        #        text = handl.handle(m.lemmatize(text))
        #        o["description"].extend(text)
        #        print(o["description"])
        prevP = node
    return bags


def compute_bigrams(words):
    bigram_list = []
    for i in range(len(words)-1):
        if words[i].istitle() and i + 1 < len(words):
            bigram_list.append(words[i] + words[i + 1])
    return bigram_list

def cosine(vec1, vec2):
    result = 1 - spatial.distance.cosine(vec1, vec2)


#fields to search:
#
# description
# key_skills (it's array), fields: name
# name
# specializations (array), fields: name, profarea_name
#area.id

def bag_of_words(vacancy, handl, m):
    if vacancy != None:
        vec = []

        bag = parse_description(vacancy["description"], handl, m)
        vec.extend(bag)


        """ for key_skill in vacancy['key_skills']:
            vec.extend(handl.handle(m.lemmatize(key_skill['name'])))
        vec.extend(handl.handle(m.lemmatize(vacancy['name'])))
        for spec in vacancy['specializations']:
            vec.extend(handl.handle(spec['name']))
            vec.extend(handl.handle(spec['profarea_name'])) """
        bigrams = compute_bigrams(bag)
        vec.extend(bigrams)
        return vec
    return None

#fields to search
#
#skill_set(array) +
#skills +
#specialization(array) profarea_name, namem profarea_id
#title (name of resume) - corresponds to vacancy name +
#experience[0].description - (months)
#area.id
def get_resume_vector(resume):
    pass

def count_all(ids):
    print("Proccess with ids from " + str(ids[0]) + " to " + str(ids[len(ids) - 1]) + " has started")
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    m = Mystem()
    freq_cash = { }
    handl = TextHandler(MorphAnalyzer())

    for id in ids:
        vac = conn.get('vacancies', { 'id' : str(id) })
        bag = bag_of_words(vac, handl, m)
        #doc = {"text" : corp}
        doc = { "id": str(id), "bag": []}

        #conn.add_or_update('corpus', doc, doc)

        #doc = { "id": str(id), "bag": bag}
        #
        if bag is not None:
            for i in range(0, len(bag)):
                doc["bag"].append(bag[i])
                if bag[i] not in freq_cash.keys():
                    f = conn.get('freqs', {"word" : bag[i]})
                    freq_cash[bag[i]] = f["freq"] if f is not None else 0
                else:
                    freq_cash[bag[i]] = freq_cash[bag[i]] + 1

        print ("Proccess with ids from " + str(ids[0]) + " to " + str(ids[len(ids) - 1]) + ": bag for vacancy " + str(vac["id"]))
        conn.add_or_update('vacancy_bags', doc, doc)
        
    for freq in freq_cash.keys():
        search = {"word" : freq}
        doc = {"word" : freq, "freq": freq_cash[freq]}
        conn.add_or_update('freqs', search, doc)
    print("Proccess with ids from " + str(ids[0]) + " to " + str(ids[len(ids) - 1]) + " has finished")
    conn.close()

def vectorize_vacancies(ids):
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    for id in ids:
        vac = conn.get('vacancies', { 'id' : str(id) })
        bag = bag_of_words(vac)



if __name__ == "__main__":
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    ids_all = conn.find_all(collection='vacancies')

    j = 0
    for i in ids_all:
         print(conn.get('vacancies', { "id": str(i["id"]) }))
         j = j + 1
         if j > 3:
             break


    print("\n\n")
   # print(conn.get_count('vacancies', 5))
    for i in ids_all:
         print(conn.get('vacancies', { "name": "Инженер-технолог" }))
         j = j + 1
         if j > 3:
             break

    exit(0)
    #print(conn.get())
    #doc = { "word": "w", "bag" : ["", "ss", "sss"]}
    #count_all(['19321072'])
    #conn.add_or_update('vacancy_bags', doc, doc)

    m = Mystem()
    handl = TextHandler(MorphAnalyzer())
    #conn = MongoConnection("127.0.0.1", 27017)
   # conn.connect('hh-crawler')

    pool_count = 12
    vacancies_count = conn.count('vacancies')
    vacancies_inc = int(vacancies_count / pool_count) + 1
    vacancies = 0

    ids = []
    #count_all(ids)
    #exit(0)
    from_v = 0
    to = vacancies_inc
    id_list = []
    i = 0
    import random




    for id in ids_all:
        if from_v >= vacancies_count:
                break

        if from_v > to:
            from_v = from_v + 1
            to = from_v + vacancies_inc

            ids.append(id_list)
            id_list = []
        else:
            from_v = from_v + 1
            idd = id["id"]
            v = conn.get('vacancies', { "id": str(idd) })
            print(v)
            i = i + 1

            if i > random.randint(2,5):
                break
            id_list.append(i)




   # with Pool(processes=pool_count) as pool:
   #     pool.map(count_all, ids)
   #     pool.close()
   #     pool.join()


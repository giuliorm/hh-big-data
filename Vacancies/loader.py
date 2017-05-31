__author__ = 'JuriaSan'

import requests
from repository import MongoConnection
from multiprocessing import Pool

def load_vacancies_json(spec_id, page_num):
    return requests.get("https://api.hh.ru/vacancies?specialization=" + str(spec_id) + "&page=" + str(page_num)).json()

def load_vacancy(vac):
    url = vac['url']
    url_rez = requests.get(url).json()
    return url_rez

vacancies_collection_name = "vacancies-test"

def load_vacancies(vacs, page_id):
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')

    for vac in vacs:
        vac_item = load_vacancy(vac)
        conn.add_or_update(vacancies_collection_name, vac_item, vac_item)
        print("process: " + globals()["proccess-id"]  + " page: " + str(page_id) +
              " loaded vacancy with id "  + str(vac_item['id']))
    conn.close()

    return vacs

def load_recursively(specs, page):
    for spec_item in specs:
        try:
            specs_sub = spec_item['specializations']
            load_recursively(specs_sub, page)
        except KeyError:
            pass

        spec_id = spec_item['id']

        try:
            json = load_vacancies_json(spec_id, page)
            items = json["items"]
            load_vacancies(items, page)
        except KeyError:
            print("something wrong while loading vacancies")

def load_all_vacancies(params):
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    spec = conn.get('specializations', { 'id': params[0] })
    conn.close()
    globals()["proccess-id"] = params[0]
    print ("Process " + str(params[0]) + " started")

    for page_num in range(0, params[1]):
        load_recursively([spec], page_num)

    print ("Process " +  str(params[0])  + " finished")
    conn.close()


if __name__ == "__main__":
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    specs = conn.find_all('specializations')
    jsons = [{ "id" : spec["id"], "json": load_vacancies_json(spec["id"], 0) } for spec in specs]
    params = [(i["id"], i["json"]["pages"] - 1) for i in jsons]
    pool_count = 1

    # pool = []
    #pool_count = pool_count if pages >= 10 else pages
    #pages_count = 0
    #pages_inc = int(pages / pool_count) + 1
    #pgs = []
    #while pages_count <= pages:
    #    pgs.append((pages_count, pages + 1 if (pages_count + pages_inc) >= pages else (pages_count + pages_inc) ))
    #    pages_count = pages + 1 if (pages_count + pages_inc) >= pages else (pages_count + pages_inc + 1)



    with Pool(processes=pool_count) as pool:
        pool.map(load_all_vacancies, params)
        pool.close()
        pool.join()

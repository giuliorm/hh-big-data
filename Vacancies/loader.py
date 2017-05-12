__author__ = 'JuriaSan'

import requests
from .repository import MongoConnection
from multiprocessing import Pool

def load_vacancies_json(spec_id, page_num):
    return requests.get("https://api.hh.ru/vacancies?specialization=" + str(spec_id) + "&page=" + str(page_num)).json()

def load_vacancy(vac):
    url = vac['url']
    url_rez = requests.get(url).json()
    #vac['info'] = url_rez
    return url_rez

def load_vacancies(vacs, connection, page_id):
    for vac in vacs:
        vac_item = load_vacancy(vac)
        connection.add_or_update('vacancies', vac_item, vac_item)
        print("process: " + str(page_id) + " loaded vacancy with id "  + str(vac_item['id']))
    return vacs

def load_recursively(specs, conn, page_from, page_to):
    for spec_item in specs:
        while(page_from <= page_to):
            try:
                specs_sub = spec_item['specializations']
                load_recursively(specs_sub, conn, page_from, page_from)

            except KeyError:
                pass
            spec_id = spec_item['id']
            json = load_vacancies_json(spec_id, page_from)

            items = None
            try:
                items = json["items"]
                load_vacancies(items, conn, page_from)
            except KeyError:
                pass

            page_from = page_from + 1


def load_all_vacancies(pages):
    #1.21
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    spec = conn.get('specializations', { 'id' : '1' })
    print ("Process " + str(pages[0]) + " to " + str(pages[1]) + " started")

    load_recursively([spec], conn, pages[0], pages[1])

    print ("Process " +  str(pages[0]) + " to " + str(pages[1])  + " finished")
    conn.close()


def print_vacs(from_v, to, conn):
    for i in range(from_v, to):
        v = conn.get


if __name__ == "__main__":
    conn = MongoConnection("127.0.0.1", 27017)
    conn.connect('hh-crawler')
    spec = conn.get('specializations', { 'id':'1' })
    #load_recursively([spec], conn, 0, 0)
    #exit(0)
    json = load_vacancies_json(spec["id"], 0)
    pages = json["pages"] - 1
    pool_count = 10
    # pool = []
    pool_count = pool_count if pages >= 10 else pages
    pages_count = 0
    pages_inc = int(pages / pool_count) + 1
    pgs = []
    while pages_count <= pages:
        pgs.append((pages_count, pages + 1 if (pages_count + pages_inc) >= pages else (pages_count + pages_inc) ))
        pages_count = pages + 1 if (pages_count + pages_inc) >= pages else (pages_count + pages_inc + 1)



    with Pool(processes=pool_count) as pool:
        pool.map(load_all_vacancies, pgs)
        pool.close()
        pool.join()

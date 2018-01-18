# coding=utf-8
import json
import msvcrt
import os
import sys
import time

import psycopg2

from config import DB_HOST, DB_PORT, DB_PASSWORD, DB_USER, DB_NAME
from indexer import Indexer


def search(data, node=None):
    indexer = Indexer()
    if node:
        for i in indexer.search(data, node):
            print i
    else:
        for i in indexer.search(data):
            print i
    print


def suggest(data, node=None):
    indexer = Indexer(deepindexing=True)
    if node:
        for i in indexer.suggest(data, node, limit=10):
            print i
    else:
        for i in indexer.suggest(data, limit=10):
            print i


def index1():
    indexer = Indexer(deepindexing=True)
    indexer.create_index()
    data = open("input.txt").readlines()
    data[0] = data[0][3:]
    counter = 0
    start = time.time()
    for row in data:
        counter += indexer.index(row)
    stop = time.time()
    print "Indexed {0} elements in {1} seconds".format(counter, stop - start)


def index2():
    indexer = Indexer()
    indexer.create_index(["country_name", "county_name", "street_name"])
    data = json.load(open("sample.json"))
    start = time.time()
    counter = indexer.index(data)
    stop = time.time()
    print "Indexed {0} elements in {1} seconds".format(counter, stop - start)


def classical_search(term):
    database = psycopg2.connect(host=DB_HOST, port=DB_PORT, password=DB_PASSWORD, user=DB_USER,
                                database=DB_NAME)
    cursor = database.cursor()
    start = time.time()
    cursor.execute("select * from data.original where data like '%%' || %s || '%%'", [term])
    data = cursor.fetchall()
    stop = time.time()
    print "Found in {0} seconds!".format(stop - start)
    for i in data:
        print i
    print


def dynamic_search():
    indexer = Indexer(deepindexing=True, relevant_suggestions=False)
    r = ''
    while True:
        suggestions = []
        os.system("cls")
        print "Search:", r
        if r:
            for i in indexer.suggest(r, limit=20):
                suggestions.append(i)
                print i
        a = msvcrt.getch()
        if ord(a) == 8:
            r = r[:-1]
        elif ord(a) == 3:
            sys.exit(0)
        elif r and ord(a) == 13:
            search_element = ""
            for element in r.split()[:-1]:
                search_element += element + " "
            if suggestions:
                if r.split()[-1] in suggestions:
                    search_element += r.split()[-1]
                else:
                    search_element += suggestions[0]
            for i in indexer.search(search_element):
                print i[1]
                print '\n' * 4
                print '#' * 120
            raw_input()
        else:
            r += a


if __name__ == '__main__':
    # index1()
    # index2()
    # suggest("mp")
    dynamic_search()
    # search('craiul')
    # classical_search("paianjen")

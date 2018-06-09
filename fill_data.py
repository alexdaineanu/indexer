from time import sleep

import wikipedia
from requests import ConnectionError

from server.indexer import Indexer

counter = 0


def search():
    global counter
    resolved = []
    for ii in "abcdefghijklmnopqrstuvwxyz"[::-1]:
        while True:
            try:
                s = wikipedia.search(ii)
                break
            except ConnectionError:
                sleep(10)
        for i in s:
            while True:
                try:
                    page = wikipedia.page(i)
                    break
                except ConnectionError:
                    sleep(10)
            for j in page.content:
                while True:
                    try:
                        s2 = wikipedia.search(j)
                        break
                    except ConnectionError:
                        sleep(10)
                for k in s2:
                    while True:
                        try:
                            page2 = wikipedia.page(k)
                            content = page2.content
                            title = page2.title
                            break
                        except ConnectionError:
                            sleep(10)
                        except wikipedia.DisambiguationError:
                            continue
                    if k not in resolved:
                        resolved.append(k)
                        indexer.index(content, fields={"title": title})
                        counter += 1
                        print counter


if __name__ == '__main__':
    indexer = Indexer(deepindexing=True)
    # indexer.create_index()
    wikipedia.set_lang("ro")
    search()

# coding=utf-8
import json
import re
import time
import unicodedata

import psycopg2
from psycopg2 import sql

from config import DB_HOST, DB_PORT, DB_PASSWORD, DB_USER, DB_NAME


class Indexer:
    def __init__(self, diacritics_sensitive=False, deepindexing=False, relevant_suggestions=False,
                 host=DB_HOST, port=DB_PORT, password=DB_PASSWORD, user=DB_USER,
                 database=DB_NAME):
        self._database = psycopg2.connect(host=host, port=port, password=password, user=user,
                                          database=database)
        self._cursor = self._database.cursor()
        self._symbols = '!@#&()|[{}]:;\',?/*`~$^+=<>".-_'
        self._schema = None
        self._diacritics_sensitive = diacritics_sensitive
        self._deepindexing = deepindexing
        self._relevant_suggestions = relevant_suggestions

    def __del__(self):
        self._database.close()

    @staticmethod
    def flatten_diacritics(data):
        try:
            data = unicode(data).decode('unicode-escape')
        finally:
            return unicodedata.normalize("NFKD", data).encode("ascii", "ignore")

    def delete_schema(self):
        try:
            self._cursor.execute('''DROP SCHEMA index CASCADE''')
        except psycopg2.ProgrammingError:
            self._database.rollback()
        self._cursor.execute('''CREATE SCHEMA index''')
        self._cursor.execute('''GRANT ALL ON SCHEMA index TO postgres''')
        self._cursor.execute('''GRANT ALL ON SCHEMA index TO public''')
        self._database.commit()
        try:
            self._cursor.execute('''DROP SCHEMA data CASCADE''')
        except psycopg2.ProgrammingError:
            self._database.rollback()
        self._cursor.execute('''CREATE SCHEMA data''')
        self._cursor.execute('''GRANT ALL ON SCHEMA data TO postgres''')
        self._cursor.execute('''GRANT ALL ON SCHEMA data TO public''')
        self._database.commit()

    def _create_schema(self):
        self._cursor.execute('''
                           CREATE TABLE data.original (
                               id BIGSERIAL CONSTRAINT PK_original PRIMARY KEY,
                               data TEXT,
                               fields JSONB,
                               timestamp INT)''')
        self._cursor.execute('''
                    CREATE TABLE index._general_index (
                      id BIGSERIAL CONSTRAINT PK_general_index PRIMARY KEY,
                      term TEXT,
                      inverted_index INT[])''')
        self._cursor.execute('''CREATE UNIQUE INDEX term_idx_general_index ON index._general_index USING btree(term)''')
        self._cursor.execute('''
                    CREATE TABLE index._ngram_index(
                      id BIGSERIAL CONSTRAINT PK_ngram_index PRIMARY KEY,
                      term TEXT,
                      inverted_index INT[])''')
        self._cursor.execute('''CREATE INDEX term_idx_ngram_index ON index._ngram_index USING hash(term)''')
        if self._schema:
            for node in self._schema:
                self._cursor.execute(
                    sql.SQL('''
                        CREATE TABLE index.{table_name} (
                          id BIGSERIAL CONSTRAINT {primary_key_constraint_name} PRIMARY KEY,
                          term TEXT,
                          inverted_index INT[])'''
                            ).format(
                        table_name=sql.Identifier(node),
                        primary_key_constraint_name=sql.Identifier("PK_" + node)
                    )
                )
                self._cursor.execute(
                    sql.SQL('''
                        CREATE UNIQUE INDEX {index_name} ON index.{table_name} USING btree(term)
                    ''').format(table_name=sql.Identifier(node), index_name=sql.Identifier("term_idx_" + node))
                )

        self._database.commit()

    def create_index(self, schema=None):
        if schema:
            if not isinstance(schema, list):
                raise Exception("Schema must be a list!")
        self._schema = schema
        self.delete_schema()
        self._create_schema()

    def _tokenize(self, data):
        if self._diacritics_sensitive is False:
            data = self.flatten_diacritics(unicode(data))
        data = re.findall('([a-zA-Z0-9\-\'\"]*)', data)
        data = list(set(data))
        for i in range(len(data)):
            data[i] = data[i].lower().strip(self._symbols)
        data = list(set(data))
        data.remove("")
        try:
            data.remove("u")
        except ValueError:
            pass
        return data

    def _full_text_index(self, data, fields):
        try:
            self._cursor.execute(
                sql.SQL(
                    '''INSERT INTO data.original(data, fields, timestamp) 
                        VALUES({data}, {fields}, {timestamp})''').format(
                    data=sql.Literal(data),
                    fields=sql.Literal(json.dumps(fields)),
                    timestamp=sql.Literal(time.time())
                )
            )
            self._database.commit()
            self._cursor.execute('''SELECT MAX(id) FROM data.original''')
            _id = self._cursor.fetchone()[0]
            return self._index_node(data, "_general_index", _id)
        except psycopg2.ProgrammingError:
            raise Exception(
                "There is not index created! Please call create_index() function from indexer before indexing.")

    def _get_term_id_from_table(self, term, table_name):
        self._cursor.execute(
            sql.SQL('''SELECT id FROM index.{table_name} WHERE term = {term}''').format(
                table_name=sql.Identifier(table_name),
                term=sql.Literal(term)
            )
        )
        term_exists_in_index = self._cursor.fetchone()
        try:
            term_exists_in_index = term_exists_in_index[0]
        except TypeError:
            pass
        return term_exists_in_index

    def _index_term_in_node_with_id(self, term, node, original_content_id):
        if self._get_term_id_from_table(term, node) is not None:
            self._cursor.execute(
                sql.SQL(
                    '''UPDATE index.{table_name} SET inverted_index = array_append(inverted_index, {id})
                          WHERE term = {term}''').format(
                    table_name=sql.Identifier(node),
                    id=sql.Literal(original_content_id),
                    term=sql.Literal(term)
                )
            )
        else:
            self._cursor.execute(
                sql.SQL(
                    '''INSERT INTO index.{table_name} (term, inverted_index) VALUES({term}, {id})''').format(
                    table_name=sql.Identifier(node),
                    term=sql.Literal(term),
                    id=sql.Literal("{" + str(original_content_id) + "}")
                )
            )
        self._database.commit()

    def _index_node(self, data, node, original_content_id):
        counter = 0
        tokenized_data = self._tokenize(data)
        for term in tokenized_data:
            counter += 1
            ngram_flag = not self._get_term_id_from_table(term, node) and self._deepindexing
            self._index_term_in_node_with_id(term, node, original_content_id)
            if ngram_flag:
                ngrams = []
                for i in range(1, len(term)):
                    for j in range(len(term) - i + 1):
                        ngram = term[j:j + i]
                        ngrams.append(ngram)
                ngrams.append(term)
                ngrams = list(set(ngrams))
                term_id = self._get_term_id_from_table(term, node)
                for ngram in ngrams:
                    self._index_term_in_node_with_id(ngram, "_ngram_index", term_id)
        return counter

    def _parse_json_structure(self, data, _key, original_content_id):
        counter = 0
        if _key in self._schema:
            counter += self._index_node(str(data), _key, original_content_id)
        if isinstance(data, dict):
            for key in data.iterkeys():
                counter += self._parse_json_structure(data[key], key, original_content_id)
        if isinstance(data, list):
            for item in data:
                counter += self._parse_json_structure(item, "", original_content_id)
        return counter

    def index(self, data, fields=None):
        start = time.time()
        if isinstance(data, str):
            data = data.strip()
        if len(data) == 0:
            return 0
        counter = self._full_text_index(data, fields)
        if self._schema:
            self._cursor.execute('''SELECT MAX(id) FROM data.original''')
            _id = self._cursor.fetchone()[0]
            counter += self._parse_json_structure(data, "", _id)
        stop = time.time()
        print "TOOK " + str(stop - start) + " SECONDS!"
        return counter

    def search(self, data, node="_general_index", limit=None):
        if self._diacritics_sensitive is False:
            data = self.flatten_diacritics(data)
        print "Searching {0}...".format(data)
        data = data.split()
        start = time.time()
        results = set()

        for token in data:
            self._cursor.execute(
                sql.SQL(
                    '''SELECT inverted_index FROM index.{table_name} WHERE term = {term}''').format(
                    table_name=sql.Identifier(node),
                    term=sql.Literal(token),
                )
            )
            rows = self._cursor.fetchone()
            if rows is not None:
                if len(results) != 0:
                    results = results.intersection(set(rows[0]))
                else:
                    results = set(rows[0])
            else:
                print "No results!"
                return

        stop = time.time()
        if len(results) == 0:
            print "No results!"
            return
        else:
            self._cursor.execute(
                sql.SQL(
                    '''SELECT * FROM data.original WHERE id IN {id_list}''').format(
                    id_list=sql.Literal(tuple(results))
                )
            )
            results = self._cursor.fetchall()
            if limit:
                results = results[:limit]
            for result in results:
                yield result
            print "Found {0} results in {1} seconds!".format(len(results), stop - start)

    def suggest(self, data, node="_general_index", limit=None):
        if self._diacritics_sensitive is False:
            data = self.flatten_diacritics(data)
        data = data.split()[-1]
        results = []
        start = time.time()
        if self._relevant_suggestions or not self._deepindexing:
            if limit:
                self._cursor.execute(
                    sql.SQL(
                        '''SELECT DISTINCT(term) FROM index.{table_name} 
                            WHERE term LIKE {term} LIMIT {limit}''').format(
                        table_name=sql.Identifier(node),
                        term=sql.Literal(data + "%"),
                        limit=sql.Literal(limit)
                    )
                )
            else:
                self._cursor.execute(
                    sql.SQL(
                        '''SELECT DISTINCT(term) FROM index.{table_name} WHERE term LIKE {term}''').format(
                        table_name=sql.Identifier(node),
                        term=sql.Literal(data + "%"),
                    )
                )
            results = self._cursor.fetchall()
        if self._deepindexing:
            if limit:
                self._cursor.execute(
                    sql.SQL(
                        '''WITH Q1 AS (SELECT distinct(term) AS dterm, position ({term} in term), length(term)
                            FROM index.{node} WHERE id IN (SELECT UNNEST(inverted_index) FROM index._ngram_index
                            WHERE term = {term} LIMIT {limit}) ORDER BY position, length) 
                            SELECT dterm FROM Q1''').format(
                        term=sql.Literal(data),
                        node=sql.Identifier(node),
                        limit=sql.Literal(limit)
                    )
                )
            else:
                self._cursor.execute(
                    sql.SQL(
                        '''WITH Q1 AS (SELECT distinct(term) AS dterm, position ({term} in term), length(term)
                            FROM index.{node} WHERE id IN (SELECT UNNEST(inverted_index) FROM index._ngram_index
                            WHERE term = {term}) ORDER BY position, length)
                            SELECT dterm''').format(
                        term=sql.Literal(data),
                        node=sql.Identifier(node),
                    )
                )
        fetch_data = self._cursor.fetchall()
        if fetch_data:
            if self._relevant_suggestions:
                results += [i for i in fetch_data if i not in results]
            else:
                results = fetch_data
        stop = time.time()
        if not results:
            print "No suggestion found for {0}".format(data)
            return
        print "Suggesting took {0} seconds!".format(stop - start)
        if limit:
            results = results[:limit]
        for result in results:
            yield result[0]

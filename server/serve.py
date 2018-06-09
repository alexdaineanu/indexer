from flask import Flask, jsonify
from flask_restful import Resource, Api

from indexer import Indexer

app = Flask(__name__, static_folder="static/pages")
api = Api(app)
indexer = Indexer()


class Suggestions(Resource):
    def get(self, data):
        return jsonify({"suggestions": [i for i in indexer.suggest(data, limit=20)]})


class Search(Resource):
    def get(self, data):
        return jsonify({"results": [i for i in indexer.search(data, limit=20)]})


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/scripts/script.js")
def script():
    return app.send_static_file("script.js")


api.add_resource(Suggestions, '/api/suggestions/<data>')
api.add_resource(Search, '/api/search/<data>')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)

import json
import requests
from flask import Flask, render_template, request, abort, redirect
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from urllib.parse import quote_plus

from freebase.model import Topic, get_db_url, Label

app = Flask(__name__)
engine = create_engine(get_db_url(), poolclass=NullPool)
Session = sessionmaker(bind=engine)


@app.route('/')
def main():
    return render_template('main.html')


@app.route('/google/<path:path>')
def google(path):
    path = '/' + path
    url = google_url(Topic(mid=path))
    if url is None:
        abort(404)
    return redirect(url, code=303)


@app.route('/<path:path>')
def get_entity(path):
    path = '/' + path
    db = Session()
    try:
        if path.startswith('/m/') or path.startswith('/g/'):
            topic = db.query(Topic).filter_by(mid=path).first()
        else:
            topic = db.query(Topic).filter_by(textid=path).first()
            if topic is None:
                topic = db.query(Topic).join(Topic.keys).filter_by(key=path).first()
        if topic is None:
            abort(404)

        if topic.mid is not None and path != topic.mid:
            return redirect('/freebase{}'.format(topic.mid), code=303)  # We prefer the MID

        mimetype = request.accept_mimetypes.best_match(['text/html', 'application/ld+json', 'application/json'])
        if mimetype == 'application/json' or mimetype == 'application/ld+json':
            return app.response_class(json.dumps(topic.jsonld), mimetype=mimetype)
        else:
            return render_template('topic_display.html', topic=to_full_dict(topic))
    finally:
        db.close()


def to_simple_dict(topic):
    return {
        'id': topic.textid if topic.textid else topic.mid,
        'url': '/freebase{}'.format(topic.mid if topic.mid else topic.textid),
        'label': content_negotiation(topic.labels),
        'description': content_negotiation(topic.descriptions)
    }


def to_full_dict(topic):
    desc = to_simple_dict(topic)
    desc['canonical'] = 'http://www.freebase.com{}'.format(topic.textid if topic.textid else topic.mid)
    desc['notable_types'] = [to_simple_dict(type.type) for type in topic.types if type.notable]
    desc['other_types'] = [to_simple_dict(type.type) for type in topic.types if not type.notable]
    desc['fkeys'] = [key.key for key in topic.keys]
    desc['jsonld'] = json.dumps(topic.jsonld)
    desc['google_url'] = google_url(topic)
    desc['wikidata_uri'] = wikidata_uri(topic)
    for property in topic.property:
        if property.schema is not None:
            desc['schema'] = to_simple_dict(property.schema)
        if property.expected_type is not None:
            desc['expected_type'] = to_simple_dict(property.expected_type)
        if property.unique is not None:
            desc['unique'] = property.unique
        if property.master is not None:
            desc['master'] = to_simple_dict(property.master)
        if property.reverse is not None:
            desc['reverse'] = to_simple_dict(property.reverse)
        if property.unit is not None:
            desc['unit'] = to_simple_dict(property.unit)
        if property.delegated is not None:
            desc['delegated'] = to_simple_dict(property.delegated)
    return desc


def get_topic(**filters):
    db = Session()
    try:
        topic = db.query(Topic).filter_by(**filters).first()
        if topic is None:
            abort(404)

        mimetype = request.accept_mimetypes.best_match(['text/html', 'application/ld+json', 'application/json'])
        if mimetype == 'application/json' or mimetype == 'application/ld+json':
            return app.response_class(json.dumps(topic.jsonld), mimetype=mimetype)
        else:
            return render_template('topic_display.html', topic=to_full_dict(topic))
    finally:
        db.close()


def content_negotiation(labels):
    languages = [label.language for label in labels]
    languages.append('en')
    best_language = request.accept_languages.best_match(languages)
    for label in labels:
        if label.language == best_language:
            return label
    return None


def google_url(topic: Topic):
    label = content_negotiation(topic.labels) or wikidata_label(topic)
    if label is None or topic.mid is None:
        return None
    return 'https://www.google.com/search?kgmid={}&q={}'.format(topic.mid, quote_plus(label.value))


@lru_cache(maxsize=4096)
def wikidata_uri(topic: Topic):
    if topic.mid is None:
        return None
    query = 'SELECT DISTINCT ?item WHERE { ?item wdt:P646|wdt:P2671 "%s" }' % topic.mid
    results = requests.post('https://query.wikidata.org/sparql', data=query, headers={
        'content-type': 'application/sparql-query',
        'accept': 'application/json',
        'user-agent': 'FreebaseBrowser/0.0 (https://tools.wmflabs.org/freebase/)'
    }).json()
    items = []
    for result in results['results']['bindings']:
        items.append(result['item']['value'])
    if len(items) == 1:
        return items[0]
    else:
        return None


@lru_cache(maxsize=4096)
def wikidata_label(topic: Topic):
    if topic.mid is None:
        return None
    query = 'SELECT ?itemLabel WHERE { ?item wdt:P646|wdt:P2671 "%s" . SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } }' % topic.mid
    results = requests.post('https://query.wikidata.org/sparql', data=query, headers={
        'content-type': 'application/sparql-query',
        'accept': 'application/json',
        'user-agent': 'FreebaseBrowser/0.0 (https://tools.wmflabs.org/freebase/)'
    }).json()
    for result in results['results']['bindings']:
        return Label(value=result['itemLabel']['value'], language=result['itemLabel']['value'])
    return None

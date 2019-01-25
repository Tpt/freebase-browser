import json

from flask import Flask, render_template, request, abort, redirect
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from freebase.model import Topic, get_db_url

app = Flask(__name__)
engine = create_engine(get_db_url(), poolclass=NullPool)
Session = sessionmaker(bind=engine)


@app.route('/')
def main():
    return render_template('main.html')


@app.route('/freebase/<path:path>')
def base(path):
    return redirect(path)


@app.route('/m/<mid>')
def get_mmid(mid):
    return get_topic(mid='/m/{}'.format(mid))


@app.route('/g/<mid>')
def get_gmid(mid):
    return get_topic(mid='/g/{}'.format(mid))


@app.route('/<group>/<type>')
def get_2_step_textid(group, type):
    return get_topic(textid='/{}/{}'.format(group, type))


@app.route('/<group>/<type>/<property>')
def get_3_step_textid(group, type, property):
    return get_topic(textid='/{}/{}/{}'.format(group, type, property))


@app.route('/<space>/<group>/<type>/<property>')
def get_4_step_textid(space, group, type, property):
    return get_topic(textid='/{}/{}/{}/{}'.format(space, group, type, property))


def to_simple_dict(topic):
    return {
        'id': topic.textid if topic.textid else topic.mid,
        'url': '/freebase{}'.format(topic.textid if topic.textid else topic.mid),
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

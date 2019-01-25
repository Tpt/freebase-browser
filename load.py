import gzip
import logging
import re
import sys

import plac
from rdflib import URIRef
from rdflib.plugins.parsers.ntriples import NTriplesParser
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from freebase.model import *

type_object_id = URIRef('http://rdf.freebase.com/ns/type.object.id')
type_object_key = URIRef('http://rdf.freebase.com/ns/type.object.key')
type_object_name = URIRef('http://rdf.freebase.com/ns/type.object.name')
type_object_type = URIRef('http://rdf.freebase.com/ns/type.object.type')
common_topic_alias = URIRef('http://rdf.freebase.com/ns/common.topic.alias')
common_topic_description = URIRef('http://rdf.freebase.com/ns/common.topic.description')
common_topic_notable_types = URIRef('http://rdf.freebase.com/ns/common.topic.notable_types')

logger = logging.getLogger()


def is_interesting_key(key: str):
    if key.startswith('/authority/musicbrainz/'):
        return len(key) > 59  # We do not keep base musicbrainz keys
    if key.startswith('/en'):
        return False
    if key.startswith('/wikipedia/'):
        return '_id/' not in key and '_title/' not in key
    if key.startswith('/dataworld/'):
        return False
    return True


_decode_key_regex = re.compile('\\$([0-9A-F]{4})')


def decode_key(key: str):
    return _decode_key_regex.sub(lambda k: chr(int(k.group(1), 16)), key)


def load(
        dump_file: 'url of the Freebase RDF dump',
        mid_textid_file: 'url of the part of the Freebase RDF dump containing type.object.id relations'
):
    engine = create_engine(get_db_url())
    db = sessionmaker(bind=engine)()
    Base.metadata.create_all(engine)

    def add_to_language_column(table, s, label, max_size):
        s_topic = get_topic_from_url(db, s, True)
        if s_topic is None:
            logger.warning('Not able to get mid for label subject {}'.format(s))
            return
        if len(label) >= max_size:
            logger.error('Not able to add too long label: {}'.format(label))
            return
        try:
            db.add(table(topic_id=s_topic.id, language=label.language, value=label.value))
            db.commit()
        except IntegrityError:
            db.rollback()  # We do not care about duplicates

    def add_type(s, o, notable):
        s_topic = get_topic_from_url(db, s, True)
        if s_topic is None:
            logger.warning('Not able to get mid for type subject {}'.format(s))
            return
        o_topic = get_topic_from_url(db, o, True)
        if o_topic is None:
            logger.warning('Not able to get mid for type object {}'.format(o))
            return
        try:
            db.add(Type(topic_id=s_topic.id, type_id=o_topic.id, notable=notable))
            db.commit()
        except IntegrityError:
            db.rollback()
            if notable:
                # We add notability
                db.query(Type).filter_by(topic_id=s_topic.id, type_id=o_topic.id).update({'notable': notable})
                db.commit()

    def add_key(s, key):
        if not is_interesting_key(key):
            return False
        s_topic = get_topic_from_url(db, s, True)
        if s_topic is None:
            logger.warning('Not able to get mid for key {}'.format(s))
            return
        key = decode_key(key)
        if len(key) >= 512:
            logger.error('Not able to add too long key: {}'.format(key))
            return
        try:
            db.add(Key(topic_id=s_topic.id, key=decode_key(key)))
            db.commit()
        except IntegrityError:
            db.rollback()

    class TextIdSink:
        def triple(self, s, p, o):
            if p == type_object_id:
                s = s.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
                o = o.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
                try:
                    db.add(Topic(mid=s, textid=o))
                    db.commit()
                except IntegrityError:
                    db.rollback()
            else:
                logger.info('Unexpected triple: {} {} {}'.format(s, p, o))

    class TripleSink:
        i = 0

        def triple(self, s, p, o):
            try:
                if p == type_object_name:
                    add_to_language_column(Label, s, o, 512)
                elif p == common_topic_description:
                    add_to_language_column(Description, s, o, sys.maxsize)
                elif p == common_topic_alias:
                    add_to_language_column(Alias, s, o, 512)
                elif p == type_object_type:
                    add_type(s, o, False)
                elif p == common_topic_notable_types:
                    add_type(s, o, True)
                elif p == type_object_key:
                    add_key(s, o.value)
                self.i += 1
                if self.i % 1000000:
                    logger.info(self.i)
            except ValueError:
                pass

    with gzip.open(mid_textid_file) as fp:
        NTriplesParser(sink=TextIdSink()).parse(fp)

    with gzip.open(dump_file) as fp:
        NTriplesParser(sink=TripleSink()).parse(fp)


if __name__ == '__main__':
    plac.call(load)

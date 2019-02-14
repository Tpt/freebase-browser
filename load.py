import gzip
import logging
import re
import sys
from functools import lru_cache
from typing import Optional

from rdflib import URIRef
from rdflib.plugins.parsers.ntriples import NTriplesParser
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

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


@lru_cache(maxsize=128)
def insert_query(table):
    return table.__table__.insert()


def load(
        dump_file: 'url of the Freebase RDF dump',
        mid_textid_file: 'url of the part of the Freebase RDF dump containing type.object.id relations'
):
    engine = create_engine(get_db_url(), pool_recycle=3600)
    Base.metadata.create_all(engine)

    @lru_cache(maxsize=4096)
    def get_topic_id_from_url(url: str) -> Optional[int]:
        input_id = url.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
        db = engine.connect()
        try:
            if input_id.startswith('/m/') or input_id.startswith('/g/'):
                for topic in db.execute(Topic.__table__.select(Topic.mid == input_id)):
                    return topic[0]
                return db.execute(insert_query(Topic), mid=input_id).inserted_primary_key[0]
            else:
                if len(input_id) > MAX_VARCHAR_SIZE:
                    return None
                for topic in db.execute(Topic.__table__.select(Topic.textid == input_id)):
                    return topic[0]
                return db.execute(insert_query(Topic), textid=input_id).inserted_primary_key[0]
        finally:
            db.close()

    def db_add(table, values):
        db = engine.connect()
        try:
            db.execute(
                insert_query(table),
                values
            )
        finally:
            db.close()

    def add_to_language_column(table, s, label, max_size):
        s_topic_id = get_topic_id_from_url(s)
        if s_topic_id is None:
            logger.warning('Not able to get mid for label subject {}'.format(s))
            return
        if len(label) >= max_size:
            logger.error('Not able to add too long label: {}'.format(label))
            return
        try:
            db_add(table, {'topic_id': s_topic_id, 'language': label.language, 'value': label.value})
        except IntegrityError:
            pass  # We do not care about duplicates

    def add_type(s, o, notable):
        s_topic_id = get_topic_id_from_url(s)
        if s_topic_id is None:
            logger.warning('Not able to get mid for type subject {}'.format(s))
            return
        o_topic_id = get_topic_id_from_url(o)
        if o_topic_id is None:
            logger.warning('Not able to get mid for type object {}'.format(o))
            return
        try:
            db_add(Type, {'topic_id': s_topic_id, 'type_id': o_topic_id, 'notable': notable})
        except IntegrityError:
            if notable:
                # We add notability
                db = engine.connect()
                try:
                    db.execute(
                        Type.__table__.update()
                            .where(Type.topic_id == s_topic_id, Type.type_id == o_topic_id)
                            .values(notable=notable))
                finally:
                    db.close()

    def add_key(s, key):
        if not is_interesting_key(key):
            return False
        s_topic_id = get_topic_id_from_url(s)
        if s_topic_id is None:
            logger.warning('Not able to get mid for key {}'.format(s))
            return
        key = decode_key(key)
        if len(key) >= MAX_VARCHAR_SIZE:
            logger.error('Not able to add too long key: {}'.format(key))
            return
        try:
            db_add(Key, {'topic_id': s_topic_id, 'key': decode_key(key)})
        except IntegrityError:
            pass

    class TextIdSink:
        def triple(self, s, p, o):
            if p == type_object_id:
                s = s.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
                o = o.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
                try:
                    db_add(Topic, {'mid': s, 'textid': o})
                except IntegrityError:
                    pass
            else:
                logger.info('Unexpected triple: {} {} {}'.format(s, p, o))

    class TripleSink:
        i = 0

        def triple(self, s, p, o):
            self.i += 1
            if self.i % 1000000 == 0:
                print(self.i)
                with open('progress.txt', 'wt') as pfp:
                    pfp.write(str(self.i))

            try:
                if p == type_object_name:
                    add_to_language_column(Label, s, o, MAX_VARCHAR_SIZE)
                elif p == common_topic_description:
                    add_to_language_column(Description, s, o, sys.maxsize)
                elif p == common_topic_alias:
                    add_to_language_column(Alias, s, o, MAX_VARCHAR_SIZE)
                elif p == type_object_type:
                    add_type(s, o, False)
                elif p == common_topic_notable_types:
                    add_type(s, o, True)
                elif p == type_object_key:
                    add_key(s, o.value)
            except ValueError:
                pass

    with gzip.open(mid_textid_file) as fp:
        NTriplesParser(sink=TextIdSink()).parse(fp)

    with gzip.open(dump_file) as fp:
        if os.path.isfile('progress.txt'):
            with open('progress.txt', 'rt') as fpc:
                cursor = int(fpc.read().strip())
            logger.info('Skipping the first {} lines'.format(cursor))
            for _ in range(cursor):
                fp.readline()
        NTriplesParser(sink=TripleSink()).parse(fp)


if __name__ == '__main__':
    n = len(sys.argv[1:])
    if n < 2:
        sys.exit('usage: python %s dump_file type.object.key_file' % sys.argv[0])
    elif n == 2:
        load(sys.argv[1], sys.argv[2])
    else:
        sys.exit('Unrecognized arguments: %s' % ' '.join(sys.argv[3:]))

import gzip
import logging
import re
import sys
from functools import lru_cache
from rdflib import URIRef
from rdflib.plugins.parsers.ntriples import NTriplesParser
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, OperationalError
from typing import Optional

from freebase.model import *

type_object_id = URIRef('http://rdf.freebase.com/ns/type.object.id')
type_object_key = URIRef('http://rdf.freebase.com/ns/type.object.key')
type_object_name = URIRef('http://rdf.freebase.com/ns/type.object.name')
type_object_type = URIRef('http://rdf.freebase.com/ns/type.object.type')
common_topic_alias = URIRef('http://rdf.freebase.com/ns/common.topic.alias')
common_topic_description = URIRef('http://rdf.freebase.com/ns/common.topic.description')
common_topic_notable_types = URIRef('http://rdf.freebase.com/ns/common.topic.notable_types')
type_property_unique = URIRef('http://rdf.freebase.com/ns/type.property.unique')
type_property_expected_type = URIRef('http://rdf.freebase.com/ns/type.property.expected_type')
type_property_master_property = URIRef('http://rdf.freebase.com/ns/type.property.master_property')
type_property_reverse_property = URIRef('http://rdf.freebase.com/ns/type.property.reverse_property')
type_property_schema = URIRef('http://rdf.freebase.com/ns/type.property.schema')
type_property_unit = URIRef('http://rdf.freebase.com/ns/type.property.unit')
type_property_delegated = URIRef('http://rdf.freebase.com/ns/type.property.delegated')

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


@lru_cache(maxsize=128)
def update_query(table):
    return table.__table__.update()


def load(
        dump_file: 'url of the Freebase RDF dump',
        mid_textid_file: 'url of the part of the Freebase RDF dump containing type.object.id relations'
):
    engine = create_engine(get_db_url(), pool_recycle=3600)
    Base.metadata.create_all(engine)

    def execute_select(statement, **args):
        db = engine.connect()
        try:
            for row in db.execute(statement, **args):
                yield row
        except OperationalError:
            db.close()
            db = engine.connect()
            for row in db.execute(statement, **args):
                yield row
        finally:
            db.close()

    def execute_edit(statement, **args):
        db = engine.connect()
        try:
            return db.execute(statement, **args)
        except OperationalError:
            db.close()
            db = engine.connect()
            return db.execute(statement, **args)
        finally:
            db.close()

    @lru_cache(maxsize=4096)
    def get_topic_id_from_url(url: str) -> Optional[int]:
        input_id = url.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
        if input_id.startswith('/m/') or input_id.startswith('/g/'):
            for topic in execute_select(Topic.__table__.select(Topic.mid == input_id)):
                return topic[0]
            return execute_edit(insert_query(Topic), mid=input_id).inserted_primary_key[0]
        else:
            if len(input_id) > MAX_VARCHAR_SIZE:
                return None
            for topic in execute_select(Topic.__table__.select(Topic.textid == input_id)):
                return topic[0]
            try:
                return execute_edit(insert_query(Topic), textid=input_id).inserted_primary_key[0]
            except IntegrityError as e:
                logger.error(e)
                return None

    def add_to_language_column(table, s, label, max_size):
        s_topic_id = get_topic_id_from_url(s)
        if s_topic_id is None:
            logger.warning('Not able to get mid for label subject {}'.format(s))
            return
        if len(label) >= max_size:
            logger.error('Not able to add too long label: {}'.format(label))
            return
        try:
            execute_edit(insert_query(table), topic_id=s_topic_id, language=label.language, value=label.value)
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
            execute_edit(insert_query(Type), topic_id=s_topic_id, type_id=o_topic_id, notable=notable)
        except IntegrityError:
            if notable:
                # We add notability
                execute_edit(Type.__table__.update()
                             .where(Type.topic_id == s_topic_id)
                             .where(Type.type_id == o_topic_id)
                             .values(notable=notable))

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
            execute_edit(insert_query(Key), topic_id=s_topic_id, key=decode_key(key))
        except IntegrityError:
            pass

    def add_property_topic_id_field(field_name, s, o):
        s_topic_id = get_topic_id_from_url(s)
        if s_topic_id is None:
            logger.warning('Not able to get mid for key {}'.format(s))
            return
        o_topic_id = get_topic_id_from_url(o)
        if o_topic_id is None:
            logger.warning('Not able to get mid for key {}'.format(s))
            return
        try:
            execute_edit(insert_query(Property), topic_id=s_topic_id, **{field_name: o_topic_id})
        except IntegrityError:
            execute_edit(
                update_query(Property).values(**{field_name: o_topic_id}).where(Property.topic_id == s_topic_id))

    def add_unique(s, o):
        s_topic_id = get_topic_id_from_url(s)
        if s_topic_id is None:
            logger.warning('Not able to get mid for key {}'.format(s))
            return
        try:
            execute_edit(insert_query(Property), unique=to_bool(o), topic_id=s_topic_id)
        except IntegrityError:
            execute_edit(update_query(Property).values(unique=to_bool(o)).where(Property.topic_id == s_topic_id))

    def to_bool(s):
        s = str(s)
        if s == 'true':
            return True
        elif s == 'false':
            return False
        else:
            raise ValueError("Unexpected value: '{}'".format(s))

    class TextIdSink:
        def triple(self, s, p, o):
            if p == type_object_id:
                s = s.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
                o = o.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
                try:
                    execute_edit(insert_query(Topic), mid=s, textid=o)
                except IntegrityError:
                    pass
            else:
                logger.info('Unexpected triple: {} {} {}'.format(s, p, o))

    class TripleSink:
        def __init__(self, start_cursor=0):
            self.cursor = start_cursor

        def triple(self, s, p, o):
            self.cursor += 1
            if self.cursor % 1000000 == 0:
                print(self.cursor)
                with open('progress.txt', 'wt') as pfp:
                    pfp.write(str(self.cursor))

            try:
                """
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
                """
                if p == type_property_schema:
                    add_property_topic_id_field('schema_id', s, o)
                elif p == type_property_expected_type:
                    add_property_topic_id_field('expected_type_id', s, o)
                elif p == type_property_unique:
                    add_unique(s, o)
                elif p == type_property_master_property:
                    add_property_topic_id_field('master_id', s, o)
                elif p == type_property_reverse_property:
                    add_property_topic_id_field('reverse_id', s, o)
                elif p == type_property_unit:
                    add_property_topic_id_field('unit_id', s, o)
                elif p == type_property_delegated:
                    add_property_topic_id_field('delegated_id', s, o)
            except ValueError:
                pass

    with gzip.open(mid_textid_file) as fp:
        NTriplesParser(sink=TextIdSink()).parse(fp)

    with gzip.open(dump_file) as fp:
        cursor = 0
        progress = Path('progress.txt')
        if progress.is_file():
            with progress.open('rt') as fpc:
                cursor = int(fpc.read().strip())
            logger.info('Skipping the first {} lines'.format(cursor))
        for _ in range(cursor):
            fp.readline()
        NTriplesParser(sink=TripleSink(cursor)).parse(fp)


if __name__ == '__main__':
    n = len(sys.argv[1:])
    if n < 2:
        sys.exit('usage: python %s dump_file type.object.key_file' % sys.argv[0])
    elif n == 2:
        load(sys.argv[1], sys.argv[2])
    else:
        sys.exit('Unrecognized arguments: %s' % ' '.join(sys.argv[3:]))

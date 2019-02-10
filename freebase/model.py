import os
from functools import lru_cache

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, backref

MAX_VARCHAR_SIZE = 191


def get_db_url():
    path = os.path.join(os.path.dirname(__file__), '../database_url.txt')
    if os.path.isfile(path):
        with open(path, 'rt') as fp:
            return fp.read().strip()
    else:
        raise ValueError('You should create a database_url.txt file with the database url like sqlite:///test.db')


Base = declarative_base()


class Topic(Base):
    __tablename__ = 'topics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    mid = Column(String(13), unique=True, nullable=True)
    textid = Column(String(MAX_VARCHAR_SIZE), unique=True, nullable=True)

    @property
    def jsonld(self):
        return {
            '@context': 'http://schema.org/',
            '@id': self.uri,
            '@type': [type.type.uri for type in self.types],
            'name': [{'@value': label.value, '@language': label.language} for label in self.labels],
            'description': [{'@value': description.value, '@language': description.language} for description in
                            self.descriptions],
            'alternateName': [{'@value': alias.value, '@language': alias.language} for alias in self.aliases]
        }

    @property
    def uri(self):
        if self.textid is None:
            return 'http://rdf.freebase.com/ns/{}'.format(self.mid.replace('/m/', 'm.').replace('/g/', 'g.'))
        else:
            return 'http://rdf.freebase.com/ns/{}'.format(self.textid[1:].replace('/', '.'))


class Label(Base):
    __tablename__ = 'labels'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, backref=backref('labels', lazy=True))
    language = Column(String(5), nullable=False, primary_key=True)
    value = Column(String(MAX_VARCHAR_SIZE), nullable=False)


class Description(Base):
    __tablename__ = 'descriptions'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, backref=backref('descriptions', lazy=True))
    language = Column(String(5), nullable=False, primary_key=True)
    value = Column(Text, nullable=False)


class Alias(Base):
    __tablename__ = 'aliases'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, backref=backref('aliases', lazy=True))
    language = Column(String(5), nullable=False, primary_key=True)
    value = Column(String(MAX_VARCHAR_SIZE), nullable=False, primary_key=True)


class Type(Base):
    __tablename__ = 'types'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, foreign_keys=topic_id, backref=backref('types', lazy=True))
    type_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    type = relationship(Topic, foreign_keys=type_id)
    notable = Column(Boolean, nullable=False)


class Key(Base):
    __tablename__ = 'keys'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, backref=backref('keys', lazy=True))
    key = Column(String(MAX_VARCHAR_SIZE), nullable=False, primary_key=True)


@lru_cache(maxsize=1024)
def get_topic_from_url(db: Session, url: str, insert_if_not_exists: bool = False):
    id = url.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
    if id.startswith('/m/') or id.startswith('/g/'):
        return _get_topic_from_id(db, insert_if_not_exists, mid=id)
    else:
        return _get_topic_from_id(db, insert_if_not_exists, textid=id)


def _get_topic_from_id(db: Session, insert_if_not_exists: bool, **keys):
    for topic in db.query(Topic).filter_by(**keys):
        return topic
    for _, value in keys:
        if len(value) >= MAX_VARCHAR_SIZE / 4:
            return None
    if insert_if_not_exists:
        topic = Topic(**keys)
        db.add(topic)
        db.commit()
        return topic
    else:
        return None

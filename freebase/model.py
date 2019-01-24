import os
from functools import lru_cache

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship, backref


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

    id = Column(Integer, primary_key=True)
    mid = Column(String(13), unique=True, nullable=True)
    textid = Column(Text, unique=True, nullable=True)

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
    value = Column(Text, nullable=False, primary_key=True)


class Description(Base):
    __tablename__ = 'descriptions'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, backref=backref('descriptions', lazy=True))
    language = Column(String(5), nullable=False, primary_key=True)
    value = Column(Text, nullable=False, primary_key=True)


class Alias(Base):
    __tablename__ = 'aliases'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, backref=backref('aliases', lazy=True))
    language = Column(String(5), nullable=False, primary_key=True)
    value = Column(Text, nullable=False, primary_key=True)


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
    key = Column(Text, nullable=False, primary_key=True)


@lru_cache(maxsize=1024)
def get_topic_from_url(db: Session, url: str, insert_if_not_exists=False):
    if url.startswith('http://rdf.freebase.com/ns/m.') or url.startswith('http://rdf.freebase.com/ns/g.'):
        mid = url.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
        for topic in db.query(Topic).filter_by(mid=mid):
            return topic
        if insert_if_not_exists:
            db.add(Topic(mid=mid))
            db.commit()
            return get_topic_from_url(db, url)
        else:
            return None
    elif url.startswith('http://rdf.freebase.com/ns'):
        textid = url.replace('http://rdf.freebase.com/ns', '').replace('.', '/')
        for topic in db.query(Topic).filter_by(textid=textid):
            return topic
        if insert_if_not_exists:
            db.add(Topic(textid=textid))
            db.commit()
            return get_topic_from_url(db, url)
        else:
            return None
    else:
        raise ValueError('Illegal Freebase URL: {}'.format(url))

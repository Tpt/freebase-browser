from pathlib import Path

from sqlalchemy import Column, Integer, String, Text, ForeignKey, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

MAX_VARCHAR_SIZE = 191


def get_db_url():
    path = Path(__file__).parent.parent / 'database_url.txt'
    if path.is_file():
        with path.open('rt') as fp:
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


class Property(Base):
    __tablename__ = 'properties'

    topic_id = Column(Integer, ForeignKey('topics.id'), nullable=False, primary_key=True)
    topic = relationship(Topic, foreign_keys=topic_id, backref=backref('as_properties', lazy=True))
    schema_id = Column(Integer, ForeignKey('topics.id'))
    schema = relationship(Topic, foreign_keys=schema_id, backref=backref('properties', lazy=True))
    expected_type_id = Column(Integer, ForeignKey('topics.id'))
    expected_type = relationship(Topic, foreign_keys=expected_type_id)
    unique = Column(Boolean)
    master_id = Column(Integer, ForeignKey('topics.id'))
    master = relationship(Topic, foreign_keys=master_id)
    reverse_id = Column(Integer, ForeignKey('topics.id'))
    reverse = relationship(Topic, foreign_keys=reverse_id)
    unit_id = Column(Integer, ForeignKey('topics.id'))
    unit = relationship(Topic, foreign_keys=unit_id)
    delegated_id = Column(Integer, ForeignKey('topics.id'))
    delegated = relationship(Topic, foreign_keys=delegated_id)

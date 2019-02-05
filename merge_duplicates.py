from sqlalchemy import create_engine
from sqlalchemy.sql import text

from freebase.model import *

engine = create_engine(get_db_url())
db = engine.connect()

rows = list(db.execute(text('SELECT id,topic_id,textid FROM topics,`keys` WHERE `key` = textid AND topic_id != id')))
print('Updating {} topics'.format(len(rows)))
for (from_id, to_id, text_id) in rows:
    print('{}: {} -> {}'.format(text_id, from_id, to_id))

    db.execute(text(
        'UPDATE types AS t1 LEFT JOIN types AS t2 ON t1.topic_id = t2.topic_id AND t2.type_id = {} SET t1.type_id = {} WHERE t1.type_id = {} AND t2.topic_id IS NULL'.format(
            to_id, to_id, from_id)))
    db.execute('DELETE FROM types WHERE type_id = {}'.format(from_id))  # we drop duplicates
    for table in ['labels', 'descriptions', 'aliases', 'types', 'keys']:
        db.execute(text('UPDATE IGNORE `{}` SET topic_id = {} WHERE topic_id = {}'.format(table, to_id, from_id)))
        db.execute(text('DELETE FROM `{}` WHERE topic_id = {}'.format(table, from_id)))
    db.execute(text('DELETE FROM topics WHERE id = {}'.format(from_id)))
    db.execute(text('UPDATE topics SET textid = "{}" WHERE id = {}'.format(text_id, to_id)))
db.close()

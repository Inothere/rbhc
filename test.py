import peewee as pw
import datetime
import pymongo as pm
import time
import settings
from event_engine import Event, EventEngine

db = pw.SqliteDatabase('data.db')
client = pm.MongoClient('localhost', 27017)

mongodb = client['data-db']
#collection = mongodb.create_collection('Data')


class Data(pw.Model):
    last_price = pw.DecimalField(null=False, default=0.0)
    timestamp = pw.DateTimeField(null=False)
    created_at = pw.DateTimeField(default=datetime.datetime.now)

    class Meta:
        database = db
        db_table = 'tick_data'


def init():
    db.create_tables([Data, ])


def on_tick(e):
    print 'On tick'
    print e.dict_

if __name__ == '__main__':
    from main import register_all_handlers
    ee = EventEngine()
    register_all_handlers(ee)
    ee.start()
    time.sleep(1)
    ee.put(Event('on_tick', id=1))

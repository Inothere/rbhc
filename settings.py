import logging
import pymongo as pm

logger = logging.getLogger('rbhc')
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

client = pm.MongoClient('localhost', 27017)
db = client['data-db']

event_handlers = [{
    'name': 'on_tick',
    'func': 'test.on_tick'
}]

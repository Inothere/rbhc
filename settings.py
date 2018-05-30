import logging
import pymongo as pm
from event_engine import EventEngine
import re

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

order_maps = list()   # order manager
main_engine = EventEngine()


def register_all_handlers(ee, handlers):
    if not isinstance(ee, EventEngine):
        raise Exception('Parameter must be the instance of EventEngine')

    def parse_func(full_name):
        p = re.compile(r'^(\S+)\.([\S^.]+)$')
        m = p.match(full_name)
        return m.groups() if m else (__name__, full_name)

    for handler in handlers:
        event_name = handler.get('name')
        module, func = parse_func(handler.get('func'))
        try:
            mod = __import__(module)
            ee.register(event_name, getattr(mod, func))
        except ImportError:
            pass

register_all_handlers(main_engine, event_handlers)

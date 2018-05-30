# -*- coding: utf-8 -*-

import time
from event_engine import EventEngine
from strategies import RbhcStrategy
from custom_api import CustomTdApi
import settings
import re


def register_all_handlers(ee):
    if not isinstance(ee, EventEngine):
        raise Exception('Parameter must be the instance of EventEngine')

    def parse_func(full_name):
        p = re.compile(r'^(\S+)\.([\S^.]+)$')
        m = p.match(full_name)
        return m.groups() if m else (__name__, full_name)

    for handler in settings.event_handlers:
        event_name = handler.get('name')
        module, func = parse_func(handler.get('func'))
        try:
            mod = __import__(module)
            ee.register(event_name, getattr(mod, func))
        except ImportError:
            pass


if __name__ == '__main__':
    main_ee = EventEngine()
    st = RbhcStrategy()
    td_api = CustomTdApi(b'9999', b'118155', b'passwd', [b'rb1810', b'hc1810'])
    st.register_trader(td_api)
    td_api.RegisterFront(b'tcp://180.168.146.187:10001')
    td_api.register_strategy(st)
    td_api.Init()

    st.run()

    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print 'abort'

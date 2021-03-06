# -*- coding: utf-8 -*-

import time
import threading
from strategies import RbhcStrategy
from custom_api import CustomTdApi
import settings
import sys


if __name__ == '__main__':
    user = sys.argv[1]
    password = sys.argv[2]
    instrument_ids = sys.argv[3:]
    st = RbhcStrategy(rb=instrument_ids[0], hc=instrument_ids[1])
    td_api = CustomTdApi(b'66666', user, password)
    st.register_trader(td_api)
    td_api.RegisterFront(b'tcp://101.230.222.4:51205')
    td_api.register_strategy(st)
    td_api.Init()

    st.run()

    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print 'abort'

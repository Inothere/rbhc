# -*- coding: utf-8 -*-

import time
import threading
from strategies import RbhcStrategy
from custom_api import CustomTdApi
import settings


if __name__ == '__main__':
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

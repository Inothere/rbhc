from custom_api import MyMdApi
import time
import sys

if __name__ == '__main__':
    user = sys.argv[1]
    password = sys.argv[2]
    instrument_ids = sys.argv[3:]
    md_api = MyMdApi(b'9999', user, password, instrument_ids)

    md_api.RegisterFront(b'tcp://180.168.146.187:10011')
    md_api.Init()

    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print 'abort'
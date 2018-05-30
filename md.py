from custom_api import MyMdApi
import time

if __name__ == '__main__':
    md_api = MyMdApi(b'9999', b'118155', b'passwd', [b'rb1810', b'hc1810'])

    md_api.RegisterFront(b'tcp://180.168.146.187:10011')
    md_api.Init()

    try:
        while 1:
            time.sleep(1)
    except KeyboardInterrupt:
        print 'abort'
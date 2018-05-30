import threading
import time
from custom_api import CustomTdApi
from ctp.futures import ApiStruct
from settings import logger


def close():
    instrument_ids = [b'rb1810', b'hc1810']
    login_flag = threading.Event()
    login_flag.clear()

    td_api = CustomTdApi(b'9999', b'118155', b'passwd', instrument_ids, login_flag, api_type='close_all')
    td_api.RegisterFront(b'tcp://180.168.146.187:10001')
    td_api.Init()

    login_flag.wait()
    for id in instrument_ids:
        qry_position = ApiStruct.QryInvestorPosition(
            BrokerID=td_api.brokerID,
            InvestorID=td_api.userID,
            InstrumentID=id
        )
        td_api.requestID += 1
        td_api.ReqQryInvestorPosition(qry_position, td_api.requestID)
        logger.info('{}: position query...'.format(id))
        time.sleep(0.5)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print 'Abort'


if __name__ == '__main__':
    close()

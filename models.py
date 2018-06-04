# coding: utf-8

import threading
from settings import logger
import datetime
import pytz
import json
import re
from ctp.futures import ApiStruct
import exceptions
import pymongo as pm
from pymongo import database


class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S.%f')
        else:
            return json.JSONEncoder.default(self, o)


class Base(object):
    def __init__(self, *args, **kwargs):
        self.created_at = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))

    def to_json(self):
        return json.dumps(self.__dict__, cls=DateTimeEncoder)

    @classmethod
    def to_underline(cls, text):
        """

        :param text: str
        :return: str
        """
        result = []
        for ch in text:
            if ch.isupper():
                result.append('_')
                result.append(ch.lower())
            else:
                result.append(ch)
        return ''.join(result)


class TickData(Base):
    collection_name = 'tick'

    def __init__(self, market_data=None, *args, **kwargs):
        if not isinstance(market_data, ApiStruct.DepthMarketData):
            for k in kwargs:
                self.__setattr__(k, kwargs[k])
        else:
            super(TickData, self).__init__()
            self.last_price = market_data.LastPrice
            self.update_time = market_data.UpdateTime
            self.update_milli_sec = market_data.UpdateMillisec
            self.trading_day = market_data.TradingDay
            self.action_day = market_data.ActionDay
            self.instrument_id = market_data.InstrumentID
            self.timestamp = self._format_date(self.action_day, self.update_time, self.update_milli_sec)

    def _format_date(self, trading_day, update_time, milli_sec):
        date_str = '{} {}.{}'.format(trading_day, update_time, milli_sec)
        return datetime.datetime.strptime(date_str, '%Y%m%d %H:%M:%S.%f')

    def save_to_db(self, db):
        if not isinstance(db, database.Database):
            raise exceptions.InvalidSource
        # duplicate = db[self.__class__.collection_name].find_one({'instrument_id': self.instrument_id,
        #                                  'timestamp': self.timestamp})
        # if duplicate:
        #     return
        db[self.__class__.collection_name].save(self.__dict__)

    def async_save_to_db(self, db):
        if not isinstance(db, database.Database):
            raise exceptions.InvalidSource
        th = threading.Thread(target=self.save_to_db, args=(db,))
        th.daemon = True
        th.start()

    @classmethod
    def get_by_timestamp(cls, db, instrument_id, start, end):
        docs = db[cls.collection_name].find({
            'timestamp': {
                '$gte': start,
                '$lte': end
            },
            'instrument_id': instrument_id
        })
        if not docs:
            return []
        return [cls(**doc) for doc in docs]

    @classmethod
    def latest(cls, db, instrument_id):
        from bson.errors import InvalidDocument
        try:
            docs = db[cls.collection_name].find({'instrument_id': instrument_id}).sort('timestamp', -1).limit(1)
            return cls(**docs[0]) if docs else None
        except InvalidDocument:
            logger.error('Dirty data, {}'.format(docs))
            return None


class Position(Base):
    def __init__(self, position):
        """

        :param position: ApiStruct.InvestorPosition
        """
        super(Position, self).__init__()
        self.InstrumentID = position.InstrumentID
        self.BrokerID = position.BrokerID
        self.InvestorID = position.InvestorID
        self.PosiDirection = position.PosiDirection
        self.HedgeFlag = position.HedgeFlag
        self.YdPosition = position.YdPosition
        self.Position = position.Position

    @property
    def CloseDirection(self):
        return ApiStruct.D_Sell if self.PosiDirection == ApiStruct.PD_Long else ApiStruct.D_Buy

    @property
    def IsLong(self):
        return self.PosiDirection == ApiStruct.PD_Long

    def close_orders_for_limited_price(self, db):
        orders = list()
        if self.YdPosition > 0:
            orders.append(ApiStruct.InputOrder(
                BrokerID=self.BrokerID,
                InvestorID=self.InvestorID,
                InstrumentID=self.InstrumentID,
                OrderPriceType=ApiStruct.OPT_LimitPrice,
                Direction=self.CloseDirection,
                VolumeTotalOriginal=self.YdPosition,
                TimeCondition=ApiStruct.TC_GFD,
                VolumeCondition=ApiStruct.VC_AV,
                CombHedgeFlag=self.HedgeFlag,
                CombOffsetFlag=ApiStruct.OF_CloseYesterday,
                LimitPrice=TickData.latest(db, self.InstrumentID).last_price,
                ForceCloseReason=ApiStruct.FCC_NotForceClose,
                IsAutoSuspend=False,
                UserForceClose=False
            ))
        if self.Position > 0:
            orders.append(ApiStruct.InputOrder(
                BrokerID=self.BrokerID,
                InvestorID=self.InvestorID,
                InstrumentID=self.InstrumentID,
                OrderPriceType=ApiStruct.OPT_LimitPrice,
                Direction=self.CloseDirection,
                VolumeTotalOriginal=self.Position,
                TimeCondition=ApiStruct.TC_GFD,
                VolumeCondition=ApiStruct.VC_AV,
                CombHedgeFlag=self.HedgeFlag,
                CombOffsetFlag=ApiStruct.OF_CloseToday,
                LimitPrice=TickData.latest(db, self.InstrumentID).last_price,
                ForceCloseReason=ApiStruct.FCC_NotForceClose,
                IsAutoSuspend=False,
                UserForceClose=False
            ))
        return orders


class BaseOrder(Base):
    def __init__(self, order):
        super(BaseOrder, self).__init__()
        for attr in dir(order):
            if not callable(attr) and not attr.startswith('_'):
                self.__setattr__(attr, order.__getattribute__(attr))


class FixedArray(list):
    def __init__(self, n):
        super(FixedArray, self).__init__()
        self.size = n
        self.lock = threading.RLock()

    def __len__(self):
        with self.lock:
            return super(FixedArray, self).__len__()

    def __getitem__(self, idx):
        with self.lock:
            try:
                return super(FixedArray, self).__getitem__(idx)
            except IndexError:
                return None

    def __setitem__(self, idx, value):
        with self.lock:
            if self.__len__() >= self.size:
                self.pop(0)
            return super(FixedArray, self).__setitem__(idx, value)

    def __delitem__(self, idx):
        with self.lock:
            return super(FixedArray, self).__delitem__(idx)

    def __contains__(self, item):
        with self.lock:
            return super(FixedArray, self).__contains__(item)

    def append(self, item):
        with self.lock:
            if self.__len__() >= self.size:
                self.pop(0)
            return super(FixedArray, self).append(item)

    def pop(self, idx):
        with self.lock:
            return super(FixedArray, self).pop(idx)


class ReqRspPair(object):
    def __init__(self, request_id):
        """

        :param request_id: int
        """
        self.request_id = request_id
        self.in_order = None
        self.rtn_orders = []

    def __eq__(self, other):
        if isinstance(other, int):
            return self.request_id == other
        if isinstance(other, ReqRspPair):
            return self.request_id == other.request_id
        return False


class ObserveStatus(object):
    def __init__(self, ini_status):
        self._status = ini_status
        self._observers = [] # callbacks
        self.lock = threading.RLock()
        self.status_garbage = FixedArray(10) # store last 10 abandoned status

    @property
    def status(self):
        with self.lock:
            return self._status

    def last_status(self, idx):
        n = len(self.status_garbage)
        if n <= 0:
            return ''
        try:
            return self.status_garbage[n - idx - 1]
        except IndexError:
            return ''
    
    @status.setter
    def status(self, value):
        with self.lock:
            if self._status == value:
                # no change
                return
            self.status_garbage.append(self._status) # store garbage status
            last = self._status
            self._status = value
        for func, kwargs in self._observers:
            func(value, last, **kwargs)
    
    def register_event(self, func, kwargs):
        # kwargs is a dict parameters
        self._observers.append((func, kwargs))
        return self


def on_status_change(new_val, old_val):
    print new_val, old_val

if __name__ == '__main__':
    a = TickData('1', 1.9, '20180528', '00:01:01', 500)
    print a.timestamp

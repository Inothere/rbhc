# coding: utf-8
from ctp.futures import ApiStruct
from settings import logger, db
from models import FixedArray, ObserveStatus, TickData
import sched, time, datetime
import threading
import Queue
import json


class RbhcStrategy(object):
    def __init__(self):
        self.td_api = None
        self.rb = 'rb1810'
        self.hc = 'hc1810'
        self.status = {
            self.rb: ObserveStatus('None').register_event(self.on_status_change, {'id': self.rb}),  # 注册状态改变事件
            self.hc: ObserveStatus('None').register_event(self.on_status_change, {'id': self.hc})  # 注册状态改变事件
        }  # 1. None 2. Opening 3. InPosition 4. Closing 5. Canceling 6. SendingClosing 7. SendingOpening 8. SendingCanceling
        self.last_order_info = {
            self.rb: None,
            self.hc: None
        }
        # 报单引用
        self.order_refs = {
            self.rb: 0,
            self.hc: 0
        }
        self.session_id = 0
        self.front_id = 0

        self.open_type = '10'  # 开仓方式
        self.rate = {
            self.rb: 0.0,
            self.hc: 0.0
        }

        self.direction = {
            self.rb: ApiStruct.D_Buy,
            self.hc: ApiStruct.D_Sell
        }

        self.status_lock = threading.RLock()
        self.process_th = None
        self.cancel_th = None

    def register_trader(self, td_api):
        self.td_api = td_api

    def run(self):
        self.td_api.login_flag.wait()
        self.process_th = threading.Thread(target=self._worker)
        self.process_th.daemon = True
        self.process_th.start()
        logger.info(u'开启策略...')

    def _worker(self):
        while True:
            time.sleep(1)
            cur = datetime.datetime.now()
            if cur.second != 59:
                continue
            logger.info(u'一分钟到达，计算增长率')
            self.calc_rate()
            with self.status_lock:
                if self.status[self.rb].status == 'None' and self.status[self.hc].status == 'None':
                    # 未持仓，开仓
                    self.open_all()
                elif self.status[self.rb].status == 'InPosition' or self.status[self.hc].status == 'InPosition':
                    # 持仓中，平仓
                    self.close_all()

    def calc_rate(self, seconds=59):
        close_time = datetime.datetime.now()
        open_time = close_time + datetime.timedelta(seconds=-seconds)
        rb_data = TickData.get_by_timestamp(db, self.rb, open_time, close_time)
        hc_data = TickData.get_by_timestamp(db, self.hc, open_time, close_time)

        self.rate[self.rb] = (rb_data[-1].last_price - rb_data[0].last_price) / rb_data[0].last_price
        self.rate[self.hc] = (hc_data[-1].last_price - hc_data[0].last_price) / hc_data[0].last_price

    def open_all(self):
        self.open_type = '10' if self.rate[self.rb] > self.rate[self.hc] else '01'
        logger.info('Open all')
        # rb order
        self._order_insert(
            id=self.rb,
            direction=ApiStruct.D_Sell if self.rate[self.rb] > self.rate[self.hc] else ApiStruct.D_Buy,
            offset_flag=ApiStruct.OF_Open,
            price=TickData.latest(db, self.rb).last_price
        )
        # hc order
        self._order_insert(
            id=self.hc,
            direction=ApiStruct.D_Buy if self.rate[self.rb] > self.rate[self.hc] else ApiStruct.D_Sell,
            offset_flag=ApiStruct.OF_Open,
            price=TickData.latest(db, self.hc).last_price
        )

    def close_all(self):
        logger.info('Close all')
        self._order_insert(
            id=self.rb,
            # 开仓时为买入，则平仓时卖出，反之亦然
            direction=ApiStruct.D_Sell if self.direction[self.rb] == ApiStruct.D_Buy else ApiStruct.D_Buy,
            # direction=ApiStruct.D_Sell,
            offset_flag=ApiStruct.OF_CloseToday,
            price=TickData.latest(db, self.rb).last_price
        )
        self._order_insert(
            id=self.hc,
            # 开仓时为买入，则平仓时卖出，反之亦然
            direction=ApiStruct.D_Sell if self.direction[self.hc] == ApiStruct.D_Buy else ApiStruct.D_Buy,
            # direction=ApiStruct.D_Buy,
            offset_flag=ApiStruct.OF_CloseToday,
            price=TickData.latest(db, self.hc).last_price
        )

    def on_rsp_order_insert(self, order, rsp, request_id, is_last):
        # order is InputOrder instance
        # 只有出错了才会调用
        if rsp.ErrorID != 0:
            if order.CombOffsetFlag == ApiStruct.OF_Open:
                # 开仓请求失败, 返回上一个状态
                self.status[order.InstrumentID].status = self.status[order.InstrumentID].last_status(0)
                logger.error(u'{}: 开仓请求失败，原因: {}'.format(order.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
            else:
                # 平仓请求失败，返回上一个状态
                self.status[order.InstrumentID].status = self.status[order.InstrumentID].last_status(0)
                logger.error(u'{}: 平仓请求失败，原因: {}'.format(order.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
        else:
            if order.CombOffsetFlag == ApiStruct.OF_Open:
                # 开仓请求成功
                self.status[order.InstrumentID].status = 'Opening'
                logger.info(u'{}: 开仓请求已接受，请等待结果'.format(order.InstrumentID))
            else:
                # 平仓请求成功
                self.status[order.InstrumentID].status = 'Closing'
                logger.info(u'{}: 平仓请求已接受，请等待结果'.format(order.InstrumentID))

    def on_rsp_order_action(self, action, rsp, request_id, is_last):
        if rsp.ErrorID != 0:
            # 撤单请求失败，返回上一个状态
            self.status[action.InstrumentID].status = self.status[action.InstrumentID].last_status(0)
            logger.error(u'{}: 撤单请求失败, 原因: {}'.format(action.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
        else:
            self.status[action.InstrumentID].status = 'Canceling'
            logger.info(u'{}: 撤单请求已接受，请等待结果'.format(action.InstrumentID))

    def on_rtn_trade(self, trade):
        if trade.InstrumentID in [self.rb, self.hc]:
            self.order_refs[trade.InstrumentID] = trade.OrderRef  # 记录当前报单引用
            with self.status_lock:
                if trade.OffsetFlag == ApiStruct.OF_Open:
                    # 开仓成交，设置状态
                    self.status[trade.InstrumentID].status = 'InPosition'
                    # 设置开仓方向
                    self.direction[trade.InstrumentID] = trade.Direction
                else:
                    # 平仓成交，设置状态
                    self.status[trade.InstrumentID].status = 'None'
            logger.info(
                u'{}: Deal..., direction:{}， offset:{} '.format(trade.InstrumentID, trade.Direction, trade.OffsetFlag))

    def on_rtn_order(self, order):
        if order.InstrumentID in [self.rb, self.hc]:
            self.order_refs[order.InstrumentID] = order.OrderRef  # 记录当前报单引用
            if order.CombOffsetFlag == ApiStruct.OF_Open:
                # 开仓请求成功
                self.status[order.InstrumentID].status = 'Opening'
                logger.info(u'{}: 开仓请求已接受，请等待结果'.format(order.InstrumentID))
            else:
                # 平仓请求成功
                self.status[order.InstrumentID].status = 'Closing'
                logger.info(u'{}: 平仓请求已接受，请等待结果'.format(order.InstrumentID))
            logger.info(u'OnRtnOrder, {}'.format(order))
    def _do_after_seconds(self, sec, func, vals):
        def _do():
            time.sleep(sec)
            func(*vals)
        th = threading.Thread(target=_do)
        th.daemon = True
        th.start()

    def _order_insert(self, *args, **kwargs):
        '''
        required: id, direction, offset_flag, price
        '''
        order = ApiStruct.Order(
            BrokerID=self.td_api.brokerID,
            InvestorID=self.td_api.userID,
            InstrumentID=kwargs.get('id'),
            OrderPriceType=kwargs.get('order_price_type') if kwargs.get('order_price_type') else ApiStruct.OPT_LimitPrice,
            Direction=kwargs.get('direction'),
            VolumeTotalOriginal=kwargs.get('volume') if kwargs.get('volume') else 1,
            TimeCondition=kwargs.get('time_condition') if kwargs.get('time_condition') else ApiStruct.TC_GFD,
            VolumeCondition=kwargs.get('volume_condition') if kwargs.get('volume_condition') else ApiStruct.VC_AV,
            CombHedgeFlag=ApiStruct.HF_Speculation,
            CombOffsetFlag=kwargs.get('offset_flag'),
            LimitPrice=kwargs.get('price'),
            ForceCloseReason=ApiStruct.FCC_NotForceClose,
            IsAutoSuspend=False,
            UserForceClose=False
        )
        self.td_api.requestID += 1
        self.td_api.ReqOrderInsert(order, self.td_api.requestID)
        kw = 'Opening' if kwargs.get('offset_flag') == ApiStruct.OF_Open else 'Closing'
        # 保存本次order信息，以便重新发单
        with self.status_lock:
            self.last_order_info[kwargs.get('id')] = order
            self.status[kwargs.get('id')].status = 'Sending' + kw

    def cancel(self, id):
        logger.info('Cancel {}, order_ref is {}'.format(id, self.order_refs[id]))
        order_action = ApiStruct.OrderAction(
            InstrumentID=id,
            BrokerID=self.td_api.brokerID,
            InvestorID=self.td_api.userID,
            OrderRef=self.order_refs[id],
            SessionID=self.session_id,
            FrontID=self.front_id,
            ActionFlag=ApiStruct.AF_Delete
        )
        self.td_api.requestID += 1
        self.td_api.ReqOrderAction(order_action, self.td_api.requestID)
        with self.status_lock:
            self.status[id].status = 'SendingCanceling'

    def on_status_change(self, new_status, old_status, **kwargs):
        # SendingOpening -> Opening, 3s后撤单
        # SendingClosing -> Closing 3s后撤单
        # Canceling -> None 立刻发单
        id = kwargs.get('id')
        logger.debug('{}: {} new status is {}'.format('on_status_change', id, new_status))
        if new_status == 'SendingOpening':
            self._do_after_seconds(3, self.cancel, (id,))
        elif new_status == 'SendingClosing':
            self._do_after_seconds(3, self.cancel, (id,))
        elif new_status == 'None' and old_status == 'Canceling':
            if not self.last_order_info[id]:
                return
            self._order_insert(
                id=id,
                direction=self.last_order_info[id].Direction,  # 采用上一次发单方向
                offset_flag=self.last_order_info[id].CombOffsetFlag[0],  # 采用上一次开平选项
                price=TickData.latest(db, id).last_price
            )

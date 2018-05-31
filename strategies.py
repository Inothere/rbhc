# coding: utf-8
from ctp.futures import ApiStruct
from settings import logger, db
from models import ObserveStatus, TickData
import time, datetime
import threading


class RbhcStrategy(object):
    def __init__(self):
        self.td_api = None
        self.rb = 'rb1810'
        self.hc = 'hc1810'
        self.status = {
            self.rb: ObserveStatus('None').register_event(self.on_status_change, {'id': self.rb}),  # 注册状态改变事件
            self.hc: ObserveStatus('None').register_event(self.on_status_change, {'id': self.hc})  # 注册状态改变事件
        }  # 1. None 2. Processing 3. Traded 4. Canceled
        self.last_order_info = {
            self.rb: None,
            self.hc: None
        }
        self.last_resp_info = {
            self.rb: None,
            self.hc: None
        }
        # 报单引用
        self.order_refs = {
            self.rb: b'0',
            self.hc: b'0'
        }
        self.session_id = 0
        self.front_id = 0

        self.rate = {
            self.rb: 0.0,
            self.hc: 0.0
        }

        self.status_lock = threading.RLock()
        self.process_th = None
        self.cancel_th = None

    def inc_order_ref(self, id):
        self.order_refs[id] = '{0:13d}'.format(int(self.order_refs[id]) + 1)

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
            if self.can_open(self.rb) and self.can_open(self.hc):
                # 未持仓，开仓
                self.open_all()
            elif self.can_close(self.rb) or self.can_close(self.hc):
                # 持仓中，平仓
                self.close_all()

    def calc_rate(self, seconds=59):
        close_time = datetime.datetime.now()
        open_time = close_time + datetime.timedelta(seconds=-seconds)
        rb_data = TickData.get_by_timestamp(db, self.rb, open_time, close_time)
        hc_data = TickData.get_by_timestamp(db, self.hc, open_time, close_time)

        self.rate[self.rb] = (rb_data[-1].last_price - rb_data[0].last_price) / rb_data[0].last_price
        self.rate[self.hc] = (hc_data[-1].last_price - hc_data[0].last_price) / hc_data[0].last_price

    def can_open(self, id):
        # 非开仓交易成交，才能继续开仓
        if not self.last_resp_info[id]:
            # 没有任何返回
            return True
        return self.status[id].status == 'Traded' and self.last_resp_info[id].CombOffsetFlag != ApiStruct.OF_Open

    def can_close(self, id):
        # 开仓交易成交，才能平仓
        return self.status[id].status == 'Traded' and self.last_resp_info[id].CombOffsetFlag == ApiStruct.OF_Open

    def open_all(self):
        # self.open_type = '10' if self.rate[self.rb] > self.rate[self.hc] else '01'
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
        last_direction = self.last_resp_info[self.rb].Direction
        self._order_insert(
            id=self.rb,
            # 开仓时为买入，则平仓时卖出，反之亦然
            direction=ApiStruct.D_Sell if last_direction == ApiStruct.D_Buy else ApiStruct.D_Buy,
            offset_flag=ApiStruct.OF_CloseToday,
            price=TickData.latest(db, self.rb).last_price
        )
        last_direction = self.last_resp_info[self.hc].Direction
        self._order_insert(
            id=self.hc,
            # 开仓时为买入，则平仓时卖出，反之亦然
            direction=ApiStruct.D_Sell if last_direction == ApiStruct.D_Buy else ApiStruct.D_Buy,
            offset_flag=ApiStruct.OF_CloseToday,
            price=TickData.latest(db, self.hc).last_price
        )

    def on_rsp_order_insert(self, order, rsp, request_id, is_last):
        # order is InputOrder instance
        # 只有出错了才会调用
        if rsp.ErrorID != 0:
            if order.CombOffsetFlag == ApiStruct.OF_Open:
                # 开仓请求失败, 返回上一个状态
                logger.error(u'{}: 开仓请求失败，原因: {}'.format(order.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
            else:
                # 平仓请求失败，返回上一个状态
                logger.error(u'{}: 平仓请求失败，原因: {}'.format(order.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
        else:
            if order.CombOffsetFlag == ApiStruct.OF_Open:
                # 开仓请求成功
                logger.info(u'{}: 开仓请求已接受，请等待结果'.format(order.InstrumentID))
            else:
                # 平仓请求成功
                logger.info(u'{}: 平仓请求已接受，请等待结果'.format(order.InstrumentID))

    def on_rsp_order_action(self, action, rsp, request_id, is_last):
        if rsp.ErrorID != 0:
            # 撤单请求失败
            logger.error(u'{}: 撤单请求失败, 原因: {}'.format(action.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
        else:
            logger.info(u'{}: 撤单请求已接受，请等待结果'.format(action.InstrumentID))

    def on_rtn_trade(self, trade):
        # if trade.InstrumentID in [self.rb, self.hc]:
        #     # self.order_refs[trade.InstrumentID] = trade.OrderRef  # 记录当前报单引用
        #     with self.status_lock:
        #         if trade.OffsetFlag == ApiStruct.OF_Open:
        #             # 开仓成交，设置状态
        #             self.status[trade.InstrumentID].status = 'InPosition'
        #             # 设置开仓方向
        #             self.direction[trade.InstrumentID] = trade.Direction
        #         else:
        #             # 平仓成交，设置状态
        #             self.status[trade.InstrumentID].status = 'None'
        #     logger.info(
        #         u'{}: Deal..., direction:{}， offset:{} '.format(trade.InstrumentID, trade.Direction, trade.OffsetFlag))
        pass

    def on_rtn_order(self, order):
        if self.is_my_order(order):
            self.last_resp_info[order.InstrumentID] = order
            self.status[order.InstrumentID].status = 'Processing'
            if order.OrderStatus == ApiStruct.OST_Unknown:
                # 请求到达交易所，开启撤单线程
                self._do_after_seconds(3, self.cancel, (order.InstrumentID,))
            elif order.OrderStatus == ApiStruct.OST_AllTraded:
                # 全部成交
                self.status[order.InstrumentID].status = 'Traded'
            elif order.OrderStatus == ApiStruct.OST_Canceled:
                # 撤单成功
                self.status[order.InstrumentID].status = 'Canceled'
                # 立刻重新发单
                self._order_insert(
                    id=order.InstrumentID,
                    direction=order.Direction,
                    offset_flag=order.CombOffsetFlag,
                    price=TickData.latest(db, id).last_price
                )

    def _do_after_seconds(self, sec, func, vals):
        def _do():
            time.sleep(sec)
            func(*vals)

        th = threading.Thread(target=_do)
        th.daemon = True
        th.start()

    def is_my_order(self, order):
        """

        :param order: ApiStruct.Order
        :return: bool
        """
        return order.InstrumentID in [self.rb, self.hc] and order.SessionID == self.session_id \
               and order.FrontID == self.front_id and \
               self.last_order_info[order.InstrumentID].OrderRef == order.OrderRef

    def _order_insert(self, *args, **kwargs):
        '''
        required: id, direction, offset_flag, price
        '''
        self.inc_order_ref(kwargs.get('id'))
        order = ApiStruct.Order(
            BrokerID=self.td_api.brokerID,
            InvestorID=self.td_api.userID,
            InstrumentID=kwargs.get('id'),
            OrderPriceType=kwargs.get('order_price_type') if kwargs.get(
                'order_price_type') else ApiStruct.OPT_LimitPrice,
            Direction=kwargs.get('direction'),
            VolumeTotalOriginal=kwargs.get('volume') if kwargs.get('volume') else 1,
            TimeCondition=kwargs.get('time_condition') if kwargs.get('time_condition') else ApiStruct.TC_GFD,
            VolumeCondition=kwargs.get('volume_condition') if kwargs.get('volume_condition') else ApiStruct.VC_AV,
            CombHedgeFlag=ApiStruct.HF_Speculation,
            CombOffsetFlag=kwargs.get('offset_flag'),
            LimitPrice=kwargs.get('price'),
            ForceCloseReason=ApiStruct.FCC_NotForceClose,
            IsAutoSuspend=False,
            UserForceClose=False,
            OrderRef=self.order_refs[kwargs.get('id')]
        )
        self.td_api.requestID += 1
        self.td_api.ReqOrderInsert(order, self.td_api.requestID)
        # 保存本次input order信息，以便重新发单
        self.last_order_info[kwargs.get('id')] = order

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

    def on_status_change(self, new_status, old_status, **kwargs):
        # SendingOpening -> Opening, 3s后撤单
        # SendingClosing -> Closing 3s后撤单
        # Canceling -> None 立刻发单
        # id = kwargs.get('id')
        # logger.debug('{}: {} new status is {}'.format('on_status_change', id, new_status))
        # if new_status == 'SendingOpening':
        #     self._do_after_seconds(3, self.cancel, (id,))
        # elif new_status == 'SendingClosing':
        #     self._do_after_seconds(3, self.cancel, (id,))
        # elif new_status == 'None' and old_status == 'Canceling':
        #     if not self.last_order_info[id]:
        #         return
        #     self._order_insert(
        #         id=id,
        #         direction=self.last_order_info[id].Direction,  # 采用上一次发单方向
        #         offset_flag=self.last_order_info[id].CombOffsetFlag[0],  # 采用上一次开平选项
        #         price=TickData.latest(db, id).last_price
        #     )
        pass

# coding: utf-8
from ctp.futures import ApiStruct
from settings import logger, db
from models import ObserveStatus, TickData, BaseOrder, Position
import time, datetime
import threading
from copy import deepcopy


class RbhcStrategy(object):
    def __init__(self, rb='rb1810', hc='hc1810'):
        self.td_api = None
        self.rb = rb
        self.hc = hc
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
        self.order_ref = b'0'
        self.session_id = 0
        self.front_id = 0

        self.rate = {
            self.rb: 0.0,
            self.hc: 0.0
        }
        self.open_type = '00'  # 当前分钟计算的增长率关系, '10'代表rb开空，hc开多，'01'代表rb开多，hc开空, '00'代表不能开仓
        self.last_open_type = {
            self.rb: '00',
            self.hc: '00'
        }  # 上一分钟计算的增长率关系
        self.ref_lock = threading.RLock()

        self.canceling = {
            self.rb: False,
            self.hc: False
        }
        self.cancel_lock = {
            self.rb: threading.RLock(),
            self.hc: threading.RLock()
        }  # 撤单线程锁，与self.canceling相结合，用于判断撤单线程唯一性

        self.process_th = None
        self.position_th = None
        self.day_interval = ['09:00:00', '15:00:00']
        self.night_interval = ['21:00:00', '23:00:00']
        self.fmt = '%Y-%m-%d %H:%M:%S'
        self.force_closing = False  # 强平线程启动标志
        self.positions = {
            self.rb: list(),
            self.hc: list()
        }  # 持仓
        self.position_ev = {
            self.rb: threading.Event(),
            self.hc: threading.Event()
        }

    def inc_order_ref(self, id):
        with self.ref_lock:
            self.order_refs[id] = '{0:13d}'.format(int(self.order_refs[id]) + 1)

    def incr(self):
        with self.ref_lock:
            self.order_ref = '{}'.format(int(self.order_ref) + 1)

    def register_trader(self, td_api):
        self.td_api = td_api

    def run(self):
        self.td_api.login_flag.wait()
        self.process_th = threading.Thread(target=self._worker)
        self.process_th.daemon = True
        self.process_th.start()
        logger.info(u'开启策略..., session_id: {}, front_id: {}'.format(self.session_id, self.front_id))
        self.position_th = threading.Thread(target=self._positioner)
        self.position_th.daemon = True
        self.position_th.start()
        logger.info(u'持仓查询开启...')

    def _worker(self):
        while True:
            time.sleep(1)
            if self.date_of_end():
                # 禁止继续交易
                # 启动强平线程
                if not self.force_closing:
                    self.force_closing = True
                    self._do_after_seconds(1, self._force_close, ())
                continue
            else:
                self.force_closing = False
            cur = datetime.datetime.now()
            if cur.second != 59:
                continue
            logger.info(u'一分钟到达，计算增长率')
            if not self.calc_rate():
                logger.info(u'无行情。。。。。。。')
                continue
            self.open_type = self.calc_type()
            rb_order = self.last_resp_info[self.rb].__dict__ if self.last_resp_info[self.rb] else None
            hc_order = self.last_resp_info[self.hc].__dict__ if self.last_resp_info[self.hc] else None
            logger.debug('rb last resp: {}'.format(rb_order))
            logger.debug('hc last resp: {}'.format(hc_order))
            # 判断rb是否能够开平仓
            if self.can_open(self.rb):
                self._open(self.rb, ApiStruct.D_Sell if self.open_type == '10' else ApiStruct.D_Buy)
            elif self.can_close(self.rb):
                self._close(self.rb, ApiStruct.D_Buy if self.last_open_type[self.rb] == '10' else ApiStruct.D_Sell)
            # 判断hc是否能开平仓
            if self.can_open(self.hc):
                self._open(self.hc, ApiStruct.D_Buy if self.open_type == '10' else ApiStruct.D_Sell)
            elif self.can_close(self.hc):
                self._close(self.hc, ApiStruct.D_Sell if self.last_open_type[self.hc] == '10' else ApiStruct.D_Buy)
            # if self.can_open(self.rb) and self.can_open(self.hc):
            #     # 未持仓，开仓
            #     self.open_all()
            # elif self.can_close(self.rb) or self.can_close(self.hc):
            #     # 持仓中，平仓
            #     self.close_all()

    def _positioner(self):
        while True:
            # 每隔4秒查询持仓
            time.sleep(4)
            for _id in [self.rb, self.hc]:
                self.positions[_id] = list()  # 清空持仓缓存
                if self.position_ev[_id].is_set:
                    self.position_ev[_id].clear()  # 冻结强平线程
                qry_position = ApiStruct.QryInvestorPosition(
                    BrokerID=self.td_api.brokerID,
                    InvestorID=self.td_api.userID,
                    InstrumentID=_id
                )
                self.td_api.requestID += 1
                self.td_api.ReqQryInvestorPosition(qry_position, self.td_api.requestID)

    def _force_close(self):
        self.position_ev[self.rb].wait()
        rb_positions = deepcopy(self.positions[self.hc])
        self.position_ev[self.hc].wait()
        hc_positions = deepcopy(self.positions[self.hc])
        for position in rb_positions + hc_positions:
            # 获取平今和平昨报单
            input_orders = position.close_orders_for_limited_price(db)
            for order in input_orders:
                self.td_api.ReqOrderInsert(order)


    def calc_type(self, gap=1e-5):
        if self.rate[self.rb] - self.rate[self.hc] > gap:
            return '10'
        if self.rate[self.rb] - self.rate[self.hc] < -gap:
            return '01'
        return '00'

    def calc_rate(self, seconds=59):
        close_time = datetime.datetime.now()
        open_time = close_time + datetime.timedelta(seconds=-seconds)
        rb_data = TickData.get_by_timestamp(db, self.rb, open_time, close_time)
        hc_data = TickData.get_by_timestamp(db, self.hc, open_time, close_time)
        if not rb_data or not hc_data:
            return False

        self.rate[self.rb] = (rb_data[-1].last_price - rb_data[0].last_price) / rb_data[0].last_price
        self.rate[self.hc] = (hc_data[-1].last_price - hc_data[0].last_price) / hc_data[0].last_price
        return True

    def date_of_end(self):
        b_ret = False
        cur = datetime.datetime.now()
        m_day_interval = [datetime.datetime.strptime(
            '{}-{:0>2}-{:0>2} {}'.format(cur.year, cur.month, cur.day, h),
            self.fmt
        ) for h in self.day_interval]
        m_night_interval = [datetime.datetime.strptime(
            '{}-{:0>2}-{:0>2} {}'.format(cur.year, cur.month, cur.day, h),
            self.fmt
        ) for h in self.night_interval]
        if m_day_interval[1] - cur < datetime.timedelta(minutes=5) and cur > m_day_interval[0]:
            # 日盘即将收盘
            b_ret = True
        elif m_night_interval[1] - cur < datetime.timedelta(minutes=5) and cur > m_night_interval[0]:
            # 夜盘即将收盘
            b_ret = True
        return b_ret

    def can_open(self, id):
        # 增长率相等，不能开仓
        if self.open_type == '00':
            logger.info(u'{}: 不能开仓，原因：增长率相等'.format(id))
            return False
        # 非开仓交易成交，才能继续开仓
        if not self.last_resp_info[id]:
            # 没有任何返回
            return True
        b_ret = (self.status[id].status == 'Traded' and
                 self.last_resp_info[id].CombOffsetFlag != ApiStruct.OF_Open) or \
                (self.status[id].status == 'Canceled' and
                 self.last_resp_info[id].CombOffsetFlag == ApiStruct.OF_Open)
        return b_ret

    def can_close(self, id):
        # 增长率相等，不能开仓
        if self.open_type == '00':
            logger.info(u'{}: 不能平仓，原因：增长率相等'.format(id))
            return False
        if self.open_type == self.last_open_type[id]:
            logger.info(u'{}: 不能平仓，原因：与开仓时符号相同'.format(id))
            return False
        if not (self.status[id].status == 'Traded' and self.last_resp_info[id].CombOffsetFlag == ApiStruct.OF_Open):
            logger.info(u'{}: 不能平仓，status={}, last_comb_offset={}'.
                        format(id, self.status[id].status, self.last_resp_info[id].CombOffsetFlag))
        # 开仓交易成交，才能平仓
        return (self.status[id].status == 'Traded' and \
                        self.last_resp_info[id].CombOffsetFlag == ApiStruct.OF_Open) or \
                (self.status[id].status == 'Canceled' and \
                        self.last_resp_info[id].CombOffsetFlag != ApiStruct.OF_Open)

    def _open(self, id, direction):
        self.last_open_type[id] = self.open_type  # 记录本次开仓类型
        self._order_insert(
            id=id,
            direction=direction,
            offset_flag=ApiStruct.OF_Open,
            price=TickData.latest(db, id).last_price
        )

    def _close(self, id, direction):
        self._order_insert(
            id=id,
            direction=direction,
            offset_flag=ApiStruct.OF_CloseToday,
            price=TickData.latest(db, id).last_price
        )

    def on_rsp_position(self, position, rsp, request_id, is_last):
        _id = position.InstrumentID
        positions = self.positions[position.InstrumentID]
        positions.append(Position(position))
        if is_last:
            self.position_ev[_id].set()
            # 多头持仓
            num_long = 0
            # 空头持仓
            num_short = 0
            for item in positions:
                if item.IsLong:
                    num_long += 1
                else:
                    num_short += 1
            logger.info(u'合约: {}, 多仓数量: {}, 空仓数量: {}'.
                        format(position.InstrumentID, num_long, num_short))

    def on_rsp_order_insert(self, order, rsp, request_id, is_last):
        if rsp.ErrorID != 0:
            self.last_resp_info[order.InstrumentID] = BaseOrder(order)
            self.status[order.InstrumentID].status = 'Failed'
            if order.CombOffsetFlag == ApiStruct.OF_Open:
                # 开仓请求失败, 返回上一个状态
                logger.error(u'{}: 开仓请求失败，原因: {}'.format(order.InstrumentID, rsp.ErrorMsg.decode('gb2312')))
            else:
                # 平仓请求失败，返回上一个状态
                logger.error(u'{}: 平仓请求失败，原因: {}'.format(order.InstrumentID, rsp.ErrorMsg.decode('gb2312')))

    def on_rsp_order_action(self, action, rsp, request_id, is_last):
        if rsp.ErrorID != 0:
            # 撤单请求失败
            logger.error(u'{}: 撤单请求失败, 原因: {}'.format(action, rsp.ErrorMsg.decode('gb2312')))
            logger.info(u'rb, status={}, order_ref={}'.format(self.last_resp_info[self.rb].OrderStatus,
                                                              self.last_resp_info[self.rb].OrderRef))
            logger.info(u'hc, status={}, order_ref={}'.format(self.last_resp_info[self.hc].OrderStatus,
                                                              self.last_resp_info[self.hc].OrderRef))
        else:
            logger.info(u'{}: 撤单请求已接受，请等待结果'.format(action.InstrumentID))

    def on_rtn_trade(self, trade):
        pass

    def on_rtn_order(self, order):
        if self.is_my_order(order):
            logger.info('OnRtnOrder: id={}, OrderRef={}, OrderStatus={}'.format(order.InstrumentID, order.OrderRef,
                                                                                order.OrderStatus))
            self.last_resp_info[order.InstrumentID] = BaseOrder(order)
            self.status[order.InstrumentID].status = 'Processing'
            if order.OrderStatus == ApiStruct.OST_Unknown:
                # 请求到达交易所，开启撤单线程
                logger.info(
                    u'请求到达交易所: id={}, OrderRef={}, OrderStatus={}, offset={}'.\
                        format(order.InstrumentID,
                               order.OrderRef,
                               order.OrderStatus,
                               order.CombOffsetFlag))
                with self.cancel_lock[order.InstrumentID]:
                    if not self.canceling[order.InstrumentID]:
                        # 没有其他撤单线程存在，开启撤单线程
                        self.canceling[order.InstrumentID] = True
                        self._do_after_seconds(10, self.cancel, (BaseOrder(order),))
            elif order.OrderStatus == ApiStruct.OST_AllTraded:
                # 全部成交
                self.status[order.InstrumentID].status = 'Traded'
            elif order.OrderStatus == ApiStruct.OST_Canceled:
                # 撤单成功
                logger.info('Canceled, {}'.format(order))
                self.status[order.InstrumentID].status = 'Canceled'
                if self.force_closing and order.CombOffsetFlag != ApiStruct.OF_Open:
                    logger.info(u'{}: 强平中, 重新发平仓单'.format(order.InstrumentID))
                    self._close(order.InstrumentID, self.last_resp_info[order.InstrumentID].Direction)

    def _do_after_seconds(self, sec, func, vals):
        def _do():
            logger.info('Delay thread started..., vals={}'.format(vals))
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
               and order.FrontID == self.front_id

    def _order_insert(self, *args, **kwargs):
        '''
        required: id, direction, offset_flag, price
        '''
        self.incr()
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
            OrderRef=self.order_ref
        )
        self.td_api.requestID += 1
        self.td_api.ReqOrderInsert(order, self.td_api.requestID)
        # 保存本次input order信息，以便重新发单
        logger.info(u'提交委托单，requestID={}, order_ref={}, offset={}'.\
                    format(self.td_api.requestID, order.OrderRef, order.CombOffsetFlag))
        self.last_order_info[kwargs.get('id')] = BaseOrder(order)

    def cancel(self, order):
        """

        :param order: BaseOrder
        :return:
        """
        if not order:
            return
        id = order.InstrumentID
        try:
            if self.status[id].status == 'Traded':
                logger.info('{}: Traded, do not need cancel, offset={}'.format(id, order.CombOffsetFlag))
                return
            logger.info(
                'Canceling,id={}, order_ref={}, session_id={}, front_id={}'.format(id, order.OrderRef, order.SessionID,
                                                                                   order.FrontID))
            logger.info('last_resp: status={}, order_ref={}'.format(self.last_resp_info[id].OrderStatus,
                                                                    self.last_resp_info[id].OrderRef))
            order_action = ApiStruct.InputOrderAction(
                InstrumentID=id,
                BrokerID=self.td_api.brokerID,
                InvestorID=self.td_api.userID,
                OrderRef=order.OrderRef,
                SessionID=order.SessionID,
                FrontID=order.FrontID,
                ActionFlag=ApiStruct.AF_Delete
            )
            self.td_api.requestID += 1
            self.td_api.ReqOrderAction(order_action, self.td_api.requestID)
        finally:
            with self.cancel_lock[id]:
                self.canceling[id] = False

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

# coding: utf-8
import hashlib, os, sys, tempfile
from ctp.futures import ApiStruct, MdApi, TraderApi
from settings import logger, db
import models
import threading


class MyMdApi(MdApi):
    def __init__(self, brokerID, userID, password, instrumentIDs):
        self.requestID = 0
        self.brokerID = brokerID
        self.userID = userID
        self.password = password
        self.instrumentIDs = instrumentIDs
        self.Create()
        self.strategy = None

    def Create(self):
        dir = b''.join((b'ctp.futures', self.brokerID, self.userID))
        dir = hashlib.md5(dir).hexdigest()
        dir = os.path.join(tempfile.gettempdir(), dir, 'Md') + os.sep
        if not os.path.isdir(dir): os.makedirs(dir)
        MdApi.Create(self, os.fsencode(dir) if sys.version_info[0] >= 3 else dir)

    def register_strategy(self, strategy):
        self.strategy = strategy

    def RegisterFront(self, front):
        if isinstance(front, bytes):
            return MdApi.RegisterFront(self, front)
        for front in front:
            MdApi.RegisterFront(self, front)

    def OnFrontConnected(self):
        logger.info('Market OnFrontConnected: Login...')
        req = ApiStruct.ReqUserLogin(
            BrokerID=self.brokerID, UserID=self.userID, Password=self.password)
        self.requestID += 1
        self.ReqUserLogin(req, self.requestID)

    def OnFrontDisconnected(self, nReason):
        logger.info('OnFrontDisconnected:{}'.format(nReason))

    def OnHeartBeatWarning(self, nTimeLapse):
        logger.info('OnHeartBeatWarning: {}'.format(nTimeLapse))

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        logger.info('Market OnRspUserLogin:{}'.format(pRspInfo))
        if pRspInfo.ErrorID == 0:  # Success
            logger.info('GetTradingDay:{}'.format(self.GetTradingDay()))
            self.SubscribeMarketData(self.instrumentIDs)

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        logger.info('OnRspSubMarketData:{}'.format(pRspInfo))

    def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        logger.info('OnRspUnSubMarketData:{}'.format(pRspInfo))

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        logger.info('OnRspError:{}'.format(pRspInfo))

    def OnRspUserLogout(self, pUserLogout, pRspInfo, nRequestID, bIsLast):
        logger.info('OnRspUserLogout:{}'.format(pRspInfo))

    def OnRtnDepthMarketData(self, pDepthMarketData):
        # print('OnRtnDepthMarketData:', pDepthMarketData)
        models.TickData(pDepthMarketData).async_save_to_db(db)
        if self.strategy:
            self.strategy.on_tick(pDepthMarketData)
            # logger.info('{}: {}'.format(pDepthMarketData.InstrumentID, pDepthMarketData.LastPrice))


class CustomTdApi(TraderApi):
    def __init__(self, brokerID, userID, password, login_flag=None, api_type='normal'):
        self.requestID = 0
        self.brokerID = brokerID
        self.userID = userID
        self.password = password
        self.Create()
        self.strategy = None
        self.login_flag = login_flag if login_flag else threading.Event()

        self.login_flag.clear()
        self.api_type = api_type

    def register_strategy(self, strategy):
        self.strategy = strategy

    def RegisterFront(self, front):
        if isinstance(front, bytes):
            return TraderApi.RegisterFront(self, front)
        for front in front:
            TraderApi.RegisterFront(self, front)

    def Create(self):
        dir = b''.join((b'ctp.futures', self.brokerID, self.userID))
        dir = hashlib.md5(dir).hexdigest()
        dir = os.path.join(tempfile.gettempdir(), dir, 'Td') + os.sep
        if not os.path.isdir(dir): os.makedirs(dir)
        TraderApi.Create(self, os.fsencode(dir) if sys.version_info[0] >= 3 else dir)

    def OnFrontConnected(self):
        logger.info('Trader OnFrontConnected: Login...')
        req = ApiStruct.ReqUserLogin(
            BrokerID=self.brokerID, UserID=self.userID, Password=self.password)
        self.requestID += 1
        self.ReqUserLogin(req, self.requestID)

    def OnFrontDisconnected(self, nReason):
        logger.info('OnFrontDisconnected:{}'.format(nReason))

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        logger.info('Trader OnRspUserLogin:{}'.format(pRspInfo))
        if pRspInfo.ErrorID == 0:  # Success
            logger.info('GetTradingDay:{}'.format(self.GetTradingDay()))

        '''Sending request for settlement confirm'''
        self.requestID += 1
        req = ApiStruct.SettlementInfoConfirm(BrokerID=self.brokerID, InvestorID=self.userID)
        self.ReqSettlementInfoConfirm(req, self.requestID)
        # 记录引用
        if not self.strategy:
            return
        self.strategy.session_id = pRspUserLogin.SessionID
        self.strategy.front_id = pRspUserLogin.FrontID
        for k in self.strategy.order_refs:
            self.strategy.order_refs[k] = pRspUserLogin.MaxOrderRef

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        # for id in self.instrumentIDs:
        #     qry_position = ApiStruct.QryInvestorPositionDetail(
        #         BrokerID=self.brokerID,
        #         InvestorID=self.userID,
        #         InstrumentID=id
        #     )
        #     self.requestID += 1
        #     self.ReqQryInvestorPositionDetail(qry_position, self.requestID)
        logger.info('Settlement: {}'.format(pSettlementInfoConfirm))
        self.login_flag.set()

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        if not self.strategy:
            if pRspInfo.ErrorID != 0:
                logger.error('{}, requestID: {}'.format(pRspInfo.ErrorMsg, nRequestID).decode('gb2312'))
            return
        self.strategy.on_rsp_order_insert(pInputOrder, pRspInfo, nRequestID, bIsLast)

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        if not self.strategy:
            if pRspInfo.ErrorID != 0:
                logger.error(pRspInfo.ErrorMsg.decode('gb2312'))
            return
        self.strategy.on_rsp_order_action(pInputOrderAction, pRspInfo, nRequestID, bIsLast)

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        if not pInvestorPosition:
            # 返回None，表示没有任何持仓
            return
        if self.api_type == 'close_all':
            logger.info('isLast={}, {}'.format(bIsLast, pInvestorPosition))
            if pInvestorPosition.Position > 0 and not pInvestorPosition.LongFrozen and not pInvestorPosition.ShortFrozen:
                # 持仓且没有未成交
                close_order = ApiStruct.InputOrder(
                    BrokerID=self.brokerID,
                    InvestorID=self.userID,
                    InstrumentID=pInvestorPosition.InstrumentID,
                    OrderPriceType=ApiStruct.OPT_LimitPrice,
                    Direction=ApiStruct.D_Sell if pInvestorPosition.PosiDirection == ApiStruct.PD_Long else ApiStruct.D_Buy,
                    VolumeTotalOriginal=pInvestorPosition.Position,
                    TimeCondition=ApiStruct.TC_GFD,
                    VolumeCondition=ApiStruct.VC_AV,
                    CombHedgeFlag=ApiStruct.HF_Speculation,
                    CombOffsetFlag=ApiStruct.OF_CloseToday,
                    LimitPrice=models.TickData.latest(db, pInvestorPosition.InstrumentID).last_price,
                    ForceCloseReason=ApiStruct.FCC_NotForceClose,
                    IsAutoSuspend=False,
                    UserForceClose=False
                )
                self.requestID += 1
                self.ReqOrderInsert(close_order, self.requestID)
                logger.info(
                    'Close remaining orders, instrument: {}, requestID={}'.format(
                        pInvestorPosition.InstrumentID,
                        self.requestID))

    def OnRspQryInvestorPositionDetail(self, pInvestorPositionDetail, pRspInfo, nRequestID, bIsLast):
        if not pInvestorPositionDetail:
            return
        if self.api_type == 'close_all':
            close_order = ApiStruct.InputOrder(
                BrokerID=self.brokerID,
                InvestorID=self.userID,
                InstrumentID=pInvestorPositionDetail.InstrumentID,
                OrderPriceType=ApiStruct.OPT_LimitPrice,
                Direction=ApiStruct.D_Sell if pInvestorPositionDetail.Direction == ApiStruct.D_Buy else ApiStruct.D_Buy,
                VolumeTotalOriginal=pInvestorPositionDetail.Volume,
                TimeCondition=ApiStruct.TC_GFD,
                VolumeCondition=ApiStruct.VC_AV,
                CombHedgeFlag=ApiStruct.HF_Speculation,
                CombOffsetFlag=ApiStruct.OF_CloseToday,
                LimitPrice=models.TickData.latest(db, pInvestorPositionDetail.InstrumentID).last_price,
                ForceCloseReason=ApiStruct.FCC_NotForceClose,
                IsAutoSuspend=False,
                UserForceClose=False
            )
            self.requestID += 1
            self.ReqOrderInsert(close_order, self.requestID)
            logger.info(
                'Close remaining orders, instrument: {}, requestID={}'.format(pInvestorPositionDetail.InstrumentID,
                                                                              self.requestID))
            # logger.info('Position: {}'.format(pInvestorPositionDetail))

    def OnRtnOrder(self, pOrder):
        if not self.strategy:
            logger.info(u'OnRtnOrder: {} '.format(pOrder))
            return
        self.strategy.on_rtn_order(pOrder)
        # logger.info(u'{}: Opening..., direction:{}， offset:{} '.format(pOrder.InstrumentID, pOrder.Direction, pOrder.CombOffsetFlag))

    def OnRtnTrade(self, pTrade):
        if not self.strategy:
            logger.info(
                u'{}: Deal，direction:{}， offset:{}'.format(pTrade.InstrumentID, pTrade.Direction, pTrade.OffsetFlag))
            return
        self.strategy.on_rtn_trade(pTrade)
        # logger.info(u'{}: Deal，direction:{}， direction:{}'.format(pTrade.InstrumentID, pTrade.Direction, pTrade.OffsetFlag))

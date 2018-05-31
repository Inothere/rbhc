# coding: utf-8
from settings import orders
import threading
import time
from ctp.futures import ApiStruct


class OrderThread(threading.Thread):
    def __init__(self):
        super(OrderThread, self).__init__()

    def run(self):
        while True:
            time.sleep(0.1)
            order = orders.get()
            if order.OrderStatus == ApiStruct.OST_Unknown:
                # 开启撤单线程
                pass


class OrderManager(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.order_maps = [{
            'req': None,
            'rsp': []
        }]

    def add_request(self, input_order):
        if not isinstance(input_order, ApiStruct.InputOrder):
            return
        self.order_maps.append({
            'req': input_order,
            'rsp': []
        })

    def add_response(self, order):
        if not isinstance(order, ApiStruct.Order):
            return
        matches = [(idx, x) for (idx, x) in enumerate(self.order_maps) if x['req'].OrderRef == order.OrderRef]


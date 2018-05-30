# coding: utf-8
from event_engine import Event
from ctp.futures import ApiStruct
import models
from settings import db, order_maps


def on_tick(e):
    market_data = e.dict_.get('market_data')
    models.TickData(market_data).save_to_db(db)


def on_rtn_order(e):
    """

    :param e: Event
    :return: None
    """
    order = e.dict_.get('order')
    if not isinstance(order, ApiStruct.Order):
        return
    order_maps.append(order)


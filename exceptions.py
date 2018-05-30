# coding: utf-8
class InvalidTick(Exception):
    def __init__(self, msg):
        super(InvalidTick, self).__init__(msg)


class InvalidSource(Exception):
    pass

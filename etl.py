# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 15:29:37 2017

@author: liuning11@jd.com

"""


class Etl(object):
    _sql = "select {field} from {table} where {filter} {group}"

    def __init__(self):
        self.etl = ''

    def create(self, table, field, filter, group):
        def wrapper(func):
            print(field)
            return func

        return wrapper

    def field(self, field):
        def wrapper(func):
            print(field)
            return func

        return wrapper

    def filter(self, filter):
        def wrapper(func):
            print(filter)
            return func

        return wrapper

    def table(self, table):
        def wrapper(func):
            print(table)
            return func

        return wrapper

    def group(self, group):
        def wrapper(func):
            print(group)
            return func

        return wrapper

    def join(self, etl1, etl2, on):
        def wrapper(func):
            print(etl1, etl2, on)
            return func

        return wrapper

# -*- coding: UTF-8 -*-

"""
@file __init__.py
@brief
    Defines for database center.

Created on: 2016/1/14
"""

import os
from abc import ABCMeta, abstractmethod

"""
8888888b.            .d888 d8b
888  "Y88b          d88P"  Y8P
888    888          888
888    888  .d88b.  888888 888 88888b.   .d88b.  .d8888b
888    888 d8P  Y8b 888    888 888 "88b d8P  Y8b 88K
888    888 88888888 888    888 888  888 88888888 "Y8888b.
888  .d88P Y8b.     888    888 888  888 Y8b.          X88
8888888P"   "Y8888  888    888 888  888  "Y8888   88888P'
"""

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class DbManager():
    __metaclass__ = ABCMeta

    def __init__(self, db_path=None):
        pass

    @abstractmethod
    def count(self):
        pass

    @abstractmethod
    def dbList(self):
        pass

    @abstractmethod
    def classCount(self):
        pass

    @abstractmethod
    def addList(self, record_list):
        pass

    @abstractmethod
    def search(self, field, value, count):
        pass

    @abstractmethod
    def removePhoto(self, hash):
        pass

    @abstractmethod
    def updatePhoto(self, hash, identity):
        pass

    @abstractmethod
    def distinct_search(self, field_list, order_field):
        pass

    @abstractmethod
    def add_new_person(self, class_id, name):
        pass

    @abstractmethod
    def get_all_classes(self):
        pass



"""
8888888888                   888
888                          888
888                          888
8888888     8888b.   .d8888b 888888  .d88b.  888d888 888  888
888            "88b d88P"    888    d88""88b 888P"   888  888
888        .d888888 888      888    888  888 888     888  888
888        888  888 Y88b.    Y88b.  Y88..88P 888     Y88b 888
888        "Y888888  "Y8888P  "Y888  "Y88P"  888      "Y88888
                                                          888
                                                     Y8b d88P
                                                      "Y88P"
 """


def make_db_manager(db_file_path):
    from faceapi.database.openface import DbManagerOpenface
    return DbManagerOpenface(db_file_path)
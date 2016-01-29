# -*- coding: UTF-8 -*-

"""
@file db_manager.py
@brief
    Implement of database manager in database.

Created on: 2016/1/14
"""

import os
import re
import sqlite3
import logging

import faceapi
from faceapi import exceptions
from faceapi.utils import log_center
from faceapi.database import DbManager

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


_DB_FILE = os.path.join(faceapi.BASE_DIR, "data", "facedb.db3")
_SQL_CMD_CREATE_TAB = "CREATE TABLE IF NOT EXISTS "
_SQL_TABLE_FACE = (
                    "face_table(hash TEXT PRIMARY KEY, "
                    "name TEXT, "
                    "eigen TEXT, "
                    "img_path TEXT, "
                    "class_id INTEGER)")
_SQL_TABLE_CLASS = (
                    "class_table(class_id INTEGER PRIMARY KEY, "
                    "name TEXT)")
_SQL_GET_ALL_FACE = "SELECT * FROM face_table"
_SQL_ROWS = "SELECT COUNT(*) FROM face_table"
_SQL_ADD_FACE = (
                "INSERT or REPLACE INTO "
                "face_table "
                "VALUES(?, ?, ?, ?, ?)")
_SQL_GET_FACE_WITH_FIELD = "SELECT * FROM face_table WHERE {}={} LIMIT {}"
_SQL_DISTINCT_SEARCH = "select distinct {} from face_table order by {}"
_SQL_CLASS_COUNTS = "SELECT class_id, count(class_id) as count FROM face_table GROUP BY class_id"
_SQL_GET_ROW_BY_HASH = "SELECT * FROM face_table WHERE hash=\"{}\""
_SQL_REMOVE_PHOTO_BY_HASH = "DELETE FROM face_table WHERE hash=\"{}\""
_SQL_UPDATE_PHOTO_IDX_BY_HASH = "UPDATE face_table SET name=\"{}\", class_id={}, img_path=\"{}\" WHERE hash=\"{}\""
_SQL_GET_CLASS_ID_BY_IDX = "SELECT * FROM class_table WHERE class_id={}"
_SQL_GET_CLASSES = "SELECT * FROM class_table"
_SQL_ADD_PERSON= (
                "INSERT or REPLACE INTO "
                "class_table "
                "VALUES({}, \"{}\")")


"""
.d8888b.  888
d88P  Y88b 888
888    888 888
888        888  8888b.  .d8888b  .d8888b
888        888     "88b 88K      88K
888    888 888 .d888888 "Y8888b. "Y8888b.
Y88b  d88P 888 888  888      X88      X88
 "Y8888P"  888 "Y888888  88888P'  88888P'
 """


class DbManagerOpenface(DbManager):
    def __init__(self, db_path=_DB_FILE):
        super(DbManagerOpenface, self).__init__(db_path)
        self._db_file = db_path
        self._log = log_center.make_logger(__name__, logging.INFO)
        self._log.info("db_path: {}".format(db_path))

        dir = os.path.dirname(db_path)
        if not os.path.exists(dir):
            os.makedirs(dir)

        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_CMD_CREATE_TAB + _SQL_TABLE_FACE)
                cur.execute(_SQL_CMD_CREATE_TAB + _SQL_TABLE_CLASS)
                db.commit()
        except sqlite3.Error as e:
            self._log.error(str(e))

    def count(self):
        rows = 0
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_ROWS)
                # result = cur.fetchone()
                # rows = result[0]
                (rows, ) = cur.fetchone()
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def dbList(self):
        rows = []
        db = None
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_GET_ALL_FACE)
                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def classCount(self):
        rows = []
        db = None
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cur.execute(_SQL_CLASS_COUNTS)
                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def addList(self, record_list):
        if type(record_list) is not list:
            self._log.error("record_list is not a list type, do nothing.")
            return

        try:
            db = sqlite3.connect(self._db_file)
            cur = db.cursor()
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise exceptions.LibError(str(e))

        sql_add_list = []
        for record in record_list:
            rep_str = ",".join(str(x) for x in record.eigen)
            info = (
                record.hash, record.name,
                rep_str, record.img_path, record.class_id)
            self._log.debug("add: " + str(info))
            sql_add_list.append(info)

        try:
            cur.executemany(_SQL_ADD_FACE, sql_add_list)
        except sqlite3.Error as e:
            self._log.error(str(e))

        db.commit()
        db.close()

    def search(self, field, value, count):
        rows = []
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_GET_FACE_WITH_FIELD.format(field, value, count)
                self._log.debug("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def removePhoto(self, hash):
        self._log.info("removed: {}".format(hash))
        ## TODO
        rows = []
        deleted = False
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_GET_ROW_BY_HASH.format(hash)
                self._log.info("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e
        try:
            with sqlite3.connect(self._db_file) as db:
                toDelete = rows[0]
                cur = db.cursor()
                cmd = _SQL_REMOVE_PHOTO_BY_HASH.format(hash)
                self._log.info("sql cmd: {}".format(cmd))
                cur.execute(cmd)
                ##TODO should check to ensure removed.
                os.remove(toDelete['img_path'])
                db.commit()
                deleted = True
        except IndexError as e:
            self._log.error(str(e))
            raise e
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e
        #return rows
        return deleted

    def updatePhoto(self, hash, identity):
        self._log.info("updated: {} {}".format(identity, hash))
        ## TODO
        photoRows = []
        identityRows = []
        updatedPhoto = False
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_GET_ROW_BY_HASH.format(hash, identity)
                self._log.info("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    photoRows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_GET_CLASS_ID_BY_IDX.format(identity)
                self._log.info("sql cmd: {}".format(cmd))
                cur.execute(cmd)
                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    identityRows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e
        try:
            with sqlite3.connect(self._db_file) as db:
                toUpdate = photoRows[0]
                name = identityRows[0]
                img_path = re.sub("(?<=\/)[A-z]+(?=_[A-z0-9]+\.jpg)",name['name'],toUpdate['img_path'])
                cur = db.cursor()
                cmd = _SQL_UPDATE_PHOTO_IDX_BY_HASH.format(name['name'], identity, img_path, hash)
                self._log.info("sql cmd: {}".format(cmd))
                cur.execute(cmd)
                ##TODO should check to ensure removed.
                os.rename(toUpdate['img_path'],img_path)
                db.commit()
                updatedPhoto = True
        except IndexError as e:
            self._log.error(str(e))
            raise e
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e
        #return rows
        return updatedPhoto

    def distinct_search(self, field_list, order_field):
        rows = []
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_DISTINCT_SEARCH.format(
                                ','.join(field_list), order_field)
                self._log.debug("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def add_new_person(self, class_id, name):
        rows = []
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                self._log.info("Adding person ({}, {})to db".format(name, class_id))
                cmd = _SQL_ADD_PERSON.format(class_id, name)
                self._log.debug("sql cmd: {}".format(cmd))
                cur.execute(cmd)
                db.commit()
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows

    def get_all_classes(self):
        rows = []
        try:
            with sqlite3.connect(self._db_file) as db:
                cur = db.cursor()
                cmd = _SQL_GET_CLASSES
                self._log.debug("sql cmd: {}".format(cmd))
                cur.execute(cmd)

                columns = [column[0] for column in cur.description]
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
        except sqlite3.Error as e:
            self._log.error(str(e))
            raise e

        return rows
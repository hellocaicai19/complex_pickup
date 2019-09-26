import os
import datetime
import sqlite3
import sys
import logging
import time
import copy

# coding: utf-8

from redo import Redo
from xdr_file import XdrFile


class Flow:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.cur = self.conn.cursor()
        self.input_path = ""
        self.up_path = ""
        self.bak_path = ""
        self.fileContents = []
        self.rules = []
        self.contents = []
        self.cols = {}
        self.xdrFile = None
        self.process_id = 0
        self.output_list = []
        self.fieldlen = ""
        self.line_limit = 0
        self.temp_dn = ""
        self.config = {}

    def set_dir(self, input_path):
        self.input_path = input_path

    def add_rule(self, rule):
        self.rules.append(rule)

    def set_process_id(self, process_id):
        self.process_id = process_id
        self.temp_dn = process_id  # 将临时目录的目录名称设置为processid

    def set_bak(self, bak_path):
        self.bak_path = bak_path

    def set_fieldlen(self, fieldlen):
        self.fieldlen = fieldlen

    def set_line_limit(self, line_limit):
        self.line_limit = line_limit
 
    def set_up_path(self, up_path):
        self.up_path = up_path

    def create_table(self):
        # 在sqlite中创建表，字段、属性来自配置文件中的FieldName及FieldType
        """
        :return:
        """
        check_exist_sql = "select * from sqlite_master where name='sorttable%s'" % self.process_id
        self.cur.execute(check_exist_sql)
        exist_flag = self.cur.fetchall()
        if exist_flag is None:
            self.cur.execute("DROP TABLE sorttable%s" % self.process_id)
        b_success = False
        for rule in self.rules:
            fields = rule.get_fields()
            for key, value in fields.items():

                if key in self.cols:
                    if self.cols[key][0] == value[0] and self.cols[key][1] == value[1]:
                        continue
                    self.cols = None
                    break
                self.cols[key] = (value[0], value[1])

        if self.cols is not None and len(self.cols) > 0:
            create_sql = "CREATE TABLE sorttable%s (ID   INTEGER" % self.process_id
            for key, value in self.cols.items():
                create_sql = create_sql + "," + key + "    " + value[0]
            create_sql += ", FLAG TEXT)"
            self.cur.execute(create_sql)
            b_success = True
        return b_success

    def read_xdr_file(self, filename):
        """
        :param filename:
        :return:
        """
        self.xdrFile = XdrFile() #初始化Xderfile类 from xder_file.py
        self.xdrFile.open_xdr_file(filename, 'r')
        self.xdrFile.read_xdr_file(self.fieldlen)

    def work(self, zoo, redo_node):
        """

        :param zoo:
        :param redo_node:
        :return:
        """
        redo_info = []
        self.output_list = []
        business = self.config["common"]["business"]
        files_list = os.listdir(self.input_path)
        logging.info('files in input temp:%s' % files_list)
        self.create_table()
        arrive_time = ""
        file_num = 0
        output_filename = ""
        # 取本批次首个文件的文件名作为输出文件名
        for file in files_list:
            file_path = os.path.join(self.input_path, file)
            if os.path.isfile(file_path):
                output_filename = file
                break
        source_file_list = []  # 用来存放有效的入口文件名
        redo_info.append("begin")
        try:
            zoo.create_node(redo_node)
        except Exception as e:
            logging.error("create redo node at zk failed, err:%s" % e)
            sys.exit()
        try:
            zoo.set_node_value(redo_node, ";".join(redo_info).encode('utf-8'))
        except Exception as e:
            logging.error("write redo failed , err:%s" % e)
            sys.exit()
        for file in files_list:
            file_path = os.path.join(self.input_path, file)
            if not os.path.isfile(file_path):
                continue
            file_num += 1
            self.fileContents = []
            logging.info('begin pick file :%s' % file)
            self.read_xdr_file(file_path)
            self.contents = self.xdrFile.contents
            if len(self.contents) != 0:
                this_arrivetime = self.contents[0][134][0:8]
            else:
                this_arrivetime = arrive_time
            # 获取此文件的arrive_time,判断此文件arrive_time与上一个文件的arrive_time是否一致，若不一致，将此文件挪回入口目录
            if file_num != 1 and this_arrivetime != arrive_time:
                logging.info("there is diff arrivetime in a batch file,before arr time:%s,this arr time:%s. "
                             "move %s out" %
                             (arrive_time, this_arrivetime, file))
                target_file = file_path.replace('/' + self.temp_dn, '')
                try:
                    os.rename(file_path, target_file)
                    logging.info('move file:%s to %s' % (file_path, target_file))
                except Exception as e:
                    logging.error("MOVE FILE ERR %s" % e)
                    sys.exit()
                continue
            # 重置arrive_time
            arrive_time = this_arrivetime
          
            if self.up_path != "":
                up_file_contents = []
                for contents in self.contents:
                    try:
                        line = ";".join(contents)
                        up_file_contents.append(line)
                    except Exception as e:
                        logging.error("build upfile contents failed, err:%s" % e)
                        sys.exit()
                up_temp_path = os.path.join(self.up_path, str(self.process_id))
                if not os.path.exists(up_temp_path):
                    os.mkdir(up_temp_path)
                up_file_name = os.path.join(up_temp_path, file)
                up_file = open(up_file_name, "w")
                try:
                    up_file.writelines(up_file_contents)
                    up_file.close()
                    logging.info("write upfile %s success" % up_file_name)
                except Exception as e:
                    logging.error("write upfile failed, err:%s" % e)
                    sys.exit()
            self.file_to_db()
            for rule in self.rules:
                rule.process_id = self.process_id
                flag = rule.work(self.xdrFile, self.cur, output_filename, file)
                if flag:
                    self.output_list.extend(flag)
            source_file_list.append(file)
        self.output_list = list(set(self.output_list))
        self.cur.execute("DROP TABLE sorttable%s" % self.process_id)
        redo_info.append("end")
        zoo.set_node_value(redo_node, ";".join(redo_info).encode('utf-8'))
        #self.change_up_file(source_file_list)
        self.move_file(source_file_list)
        zoo.delete_node(redo_node)
        logging.info("end the work of this batch")

    def move_file(self,  source_files):
        """
        1.单个文件分拣完成后，将分拣出的文件从出口临时目录挪入出口目录
        2.根据配置将源文件挪出或删除
            2.1.若配置项bak_path为空，将源文件删除
            2.2.若配置项bak_path不为空，将源文件挪入bak_path
        :param source_files:有效的源文件列表
        :return:
        """

        for file in self.output_list:
            try:
                target_file = file.replace('/' + self.temp_dn, '')
                os.rename(file, target_file)
                logging.info('MOVE FILE:%s-->%s' % (file, target_file))
            except Exception as e:
                logging.error("MOVE FILE ERR %s" % e)
                sys.exit()
        if not source_files:
            logging.info("move end")
            return
        for source_file in source_files:
            sf = os.path.join(self.input_path, source_file)
            if self.bak_path == "":
                os.remove(sf)
                logging.info('delete source file %s succes' % sf)
                continue
            tf = os.path.join(self.bak_path, source_file)
            try:
                os.rename(sf, tf)
                logging.info('move source file success,%s-->%s' % (sf, tf))
            except Exception as e:
                logging.error("move source file failed, err:%s" % e)
                sys.exit()
        if self.up_path != "":
            up_temp_path = os.path.join(self.up_path, str(self.process_id))
            up_file_list = os.listdir(up_temp_path)
            for up_file in up_file_list:
                sf = os.path.join(up_temp_path, up_file)
                tf = os.path.join(self.up_path, up_file)
                try:
                    os.rename(sf, tf)
                    logging.info("move up file success,%s-->%s" % (sf, tf))
                except Exception as e:
                    logging.error("move up file failed, err:%s" % e) 
        logging.info("move end")

    def change_up_file(self, file_list):
        for file in file_list:
            file_content = []
            up_temp_path = os.path.join(self.up_path, str(self.process_id))
            if not os.path.exists(up_temp_path):
                os.mkdir(up_temp_path)
                logging.info("up temp path not exist, make it:%s" % up_temp_path)
            sf = os.path.join(self.input_path, file)
            tf = os.path.join(up_temp_path, file)
            source_file = open(sf)
            for line in source_file:
                line_list = line.split(";")
                if len(line_list) <= 1:
                    continue
                new_line = ";".join(line_list[2:])
                file_content.append(new_line)
            file = open(tf, "a")
            file.writelines(file_content)
            file.close()

    def file_to_db(self):
        """
        :return:
        """
        sql_params = []
        sql_insert = "INSERT INTO sorttable" + str(self.process_id) + " VALUES(?" + ",?" * (len(self.cols) + 1) + ")"
        _id = 0
        for line in self.contents:
            sql_param = [_id]
            for col, value in self.cols.items():
                sql_param.append(line[value[1]])
            sql_param.append("")
            sql_params.append(sql_param)
            _id += 1
        self.cur.executemany(sql_insert, sql_params)
        #  插入数据库后即刻清空contents
        self.contents = []

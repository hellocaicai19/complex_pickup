import os
import sys
import time
import re
import logging
import datetime
from kazoo.client import KazooClient


class Zookeeper:

    def __init__(self, hosts, max_merge_seq):
        logging.info('create a zookeeper object')
        self.zk = ""
        self.IsConn = False
        self.Hosts = hosts
        self.MAX_MERGE_FILE_SEQUENCE = max_merge_seq
        self.filename = ''
        self.pattern = ''
        self.process_path = ''

    def connect(self):
        """
        connect to zookeeper
        :return:zookeeper object
        """
        logging.info('try connect to zookeeper')
        self.zk = KazooClient(self.Hosts)
        try:
            self.zk.start()
        except Exception as e:
            print("connect time out")
            sys.exit()
        self.IsConn = True
        return self.zk

    def get_node(self, node_path):
        """
        get free node
        :return: process_id
        """
        self.connect()
        logging.info('connect zookeeper success')
        self.process_path = node_path

        node_list = []
        if not (self.zk.exists(node_path)):
            logging.error('zookeeper process node path: %s not exist' % node_path)
            sys.exit()
        childs = self.zk.get_children(node_path)
        # len = 0
        p1 = re.compile(r"^process")
        for c in childs:
            if re.findall(p1, c):
                node_list.append(c)

        # len += 1
        node_list = sorted(node_list)
        while 1:
            for node in node_list:
                node_name = '%s/%s' % (node_path, node)
                n_child = self.zk.get_children(node_name)
                for nc in n_child:
                    if nc == 'lock':
                        continue
                lock_node = "%s/%s" % (node_name, 'lock')
                self.zk.create(lock_node, ephemeral=True)
                process_id = ''.join(node.split('_')[1:])
                logging.info('get process_id :%s from zookeeper ' % process_id)
                return process_id
            time.sleep(5)

    def lock(self, lock):
        """
        lock the free node
        :param lock:
        :return:
        """
        self.zk.create(lock, ephemeral=True)

    def get_config(self, config_path, config_node):
        """
        generate config files based on node's information
        :param config_path:
        :param config_node:
        :return:
        """
        data, stat = self.zk.get(config_node)
        with open(config_path + "config.ini", 'w') as f:
            f.writelines(data.decode())

    def cp(self, src, dest):
        """
        copy the local file to zookeeper
        :param src:local file
        :param dest:zookeeper node
        :return:
        """
        if not os.path.isfile(src):
            print("%s: `%s': Local file does not exist" % ('cp', src))
            sys.exit()

        file_size = os.path.getsize(src)
        if file_size > 1048576:
            print("%s: `%s': Local file maximum limit of 1M" % ('cp', src))
            sys.exit()

        self.connect()
        if self.zk.exists(dest):
            print("%s: `%s': Zookeeper exists" % ('cp', dest))
            sys.exit()

        with open(src, 'rb') as file:
            data = file.read()

        self.zk.create(dest)
        self.zk.set(dest, value=data)

    def zk_rename(self):
        pass

    def zk_get_merge_fn(self, cur_seq, filename_pool, business):
        """
        get zookeeper seq
        :param cur_seq:
        :param filename_pool:
        :return: zk_seq
        """
        if not self.zk.exists(filename_pool):
            logging.error('the zookeeper filename_pool not exist')
            sys.exit()
        child = self.zk.get_children(filename_pool)
        if not child:
            logging.error('the zookeeper filename_pool is empty')
            sys.exit()
        zk_fn_seq = child[0]
        file_date, zk_seq = zk_fn_seq.split('.')
        file_date = re.sub(business, "", file_date)
        zk_fs = ("%s%s" % (file_date, zk_seq))
        zk_fs = re.sub("[A-Za-z.]", "", zk_fs)
        if int(zk_fs) > int(cur_seq):
            logging.info('zk_seq > cur_seq, wait...')
            return None
        zk_seq = int(zk_seq) + 1
        if zk_seq > self.MAX_MERGE_FILE_SEQUENCE:
            zk_seq = 0
            file_date = datetime.datetime.strptime(file_date, '%Y%m%d')
            next = file_date + datetime.timedelta(days=1)
            file_date = ('%s%02d%02d' % (next.year, next.month, next.day))

        zk_seq = "%03d" % zk_seq
        next_child = '%s.%s' % (file_date, zk_seq)
        self.zk.delete("%s/%s" % (filename_pool, zk_fn_seq))
        self.zk.create("%s/%s" % (filename_pool, next_child))

        return next_child


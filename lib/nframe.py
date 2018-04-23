#!/usr/bin/env python3

# encoding: utf-8

"""

    @author: lipd

    @file: nframe.py

    @time: 2017/8/2 10:03

    @desc:
    @modification:
        1.2017/8/17 change log level (DEBUG --> INFO)
"""

import sys
import logging
import logging.handlers
import os
import configparser

import time
from optparse import OptionParser
# from zkclient import zkClient


class PublicLib(object):
    """

    """

    def __init__(self):
        self.std_err = "I am error"
        self.module = None
        self.config_name = ""
        self.logger = None

    @staticmethod
    def get_parameters():
        usage = "usage: %prog [options] PERSON_NAME"
        version = "%prog 1.0.0"
        parser = OptionParser(usage=usage, version=version)

        parser.add_option('-c', '--config',
                          dest='CONFIG',
                          help='your config file')

        (options, args) = parser.parse_args()
        config_file = options.CONFIG

        if not os.path.isfile(config_file):
            print('config file does not exist')
            sys.exit(1)

    def set_log(self, logpath, logname):
        """
        :param logpath:
        :param logname:

        :return:
        """
        if not os.path.exists(logpath):
            print("logpath:%s not exist!" % logpath)
            sys.exit()
        # level = 'INFO'
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        filename = logpath + '/' + logname + time.strftime('%Y%m%d', time.localtime(time.time())) + '.log'
        fh = logging.handlers.TimedRotatingFileHandler(filename, 'D', 1, 0)
        fh.suffix = "%Y%m%d-%H%M%S.log"
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s "
            "[%(module)s.%(funcName)s:%(lineno)d] ""%(message)s")
        fh.setFormatter(formatter)

        self.logger.addHandler(fh)
        return self.logger

    @staticmethod
    def write_redo(redo_path, redo, session, on_zk=False, zk_host_list=None, zk_redo_file=None):
        """

        :param redo_path:
        :param redo:
        :param session:
        :param on_zk:
        :param zk_host_list:
        :param zk_redo_file:
        :return:
        """
        if not session:
            flag = False
        else:
            try:
                if not os.path.exists(redo_path):
                    logging.info("%s is not exists!", redo_path)
                    sys.exit()
                rfn = open(os.path.join('%s%s' % (redo_path, redo)), 'a')
                rfn.write(session)
                '''
                if on_zk is True:
                    try:
                        zk = zkClient.zkClient(zk_host_list)
                        zk.save(zk_redo_file, redo_msg)

                    except Exception as e:
                        raise Exception("[ERROR] >>>write zk redo file failed!!!<<< ERROR_INFO: %s" % str(e))

                    finally:
                        zk.close()
                '''
                rfn.close()
                flag = True

            except IOError as e:
                logging.info(e)
                flag = False
        return flag

    @staticmethod
    def read_redo(redo_path, redo):
        """

        :param redo_path:
        :param redo:
        :return:
        """
        redo_file = os.path.join('%s%s' % (redo_path, redo))
        if not os.path.exists(redo_file):
            logging.info("redo file %s not exist" % redo_file)
            return False
        with open(redo_file) as file:
            pass
        return

    @staticmethod
    def delete_redo(redo_path, redo, on_zk=False, zk_host_list=None, zk_redo_file=None):
        """

        :param redo_path:
        :param redo:
        :param on_zk:
        :param zk_host_list:
        :param zk_redo_file:
        :return:
                1:delete successed
               -1:delete failed
        """
        redo_file = os.path.join('%s%s' % (redo_path, redo))
        if not os.path.exists(redo_file):
            logging.info("redo file:%s not exists" % redo_file)
            return False
        os.remove(redo_file)
        logging.info("delete redo file %s" % redo_file)
        return True

    def search_file(self):
        pass


class ConfigParser(object):

    def __init__(self, config_file):
        self.config = config_file

    def get_config(self):
        """
        :return: config object
        """
        config = {}
        cf = configparser.ConfigParser()
        cf.read(self.config)
        sections = []
        for section in cf.sections():
            sections.append(section)
            items = {}
            for key, value in cf.items(section):
                items[key] = value
                config[section] = items
        return config

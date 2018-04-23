#!/usr/bin/python
# -*- coding:utf-8 -*-

import abc

class BaseInterfaces(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write_redo(self, redo_file, redo_content_dic, on_zk=False, zk_host_list=None, zk_redo_file=None):
        pass

    @abc.abstractmethod
    def delete_redo(self, redo_file, on_zk=False, zk_host_list=None, zk_redo_file=None):
        pass
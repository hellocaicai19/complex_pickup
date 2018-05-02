#!/usr/bin/env python3

# encoding: utf-8

'''

@author: lipd

@file: test_zk.py

@time: 2018/4/26 10:17

@desc:

'''
import sys
sys.path.append("..")

from zookeeper import Zookeeper
list_redo = ["step:begin", "filename:test4,test2"]
zoo = Zookeeper("10.12.1.174:2181,10.12.1.171:2181,10.12.1.234:2181", None)
zk_node = "/nonzc/test"
zoo.test_transaction()
# node = zoo.get_node(zk_node)
# process_id = ''.join(node.split('_')[1:])
# print(process_id)
# # flag1 = zoo.create_node(zk_node + "/" + node + "/" + "redo")
# # print(flag1)
# flag2 = zoo.set_node_value(zk_node + "/" + node + "/" + "redo", ";".join(list_redo).encode('utf-8'))
# print(flag2)
# data, stat = zoo.get_node_value(zk_node + "/" + node + "/" + "redo")
#
# print("data:", data)
# print("stat", stat)

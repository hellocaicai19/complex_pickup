#!/usr/bin/env python3

# encoding: utf-8

"""

@author: lipd

@file: get_config

@time: 2018/5/6 16:07

@desc:
"""
import os
import sys
import subprocess
from zookeeper import Zookeeper
zk_nodelist = "10.12.1.174:2181,10.12.1.171:2181,10.12.1.234:2181"
zk_process_id_path = "/nonzc/mms/pick"
zk_config_node = "/home/nrjfpaas/asiainfo/lipd/non_zc/app/complex_pick/config/mms/mms_pick.ini"
local_config_path = "f:/test_config"

# zk_nodelist = os.environ("zk_node_list")
# zk_process_id_path = os.environ("zk_process_id_path")
# zk_config_node = os.environ("zk_config_node")
# local_config_path = os.environ("local_config_path")

zoo = Zookeeper(zk_nodelist, None)
process_id = zoo.get_node(zk_process_id_path)
process_id = ''.join(process_id.split('_')[1:])
config_file_name = process_id + "_" + os.path.basename(zk_config_node)
local_config_name = local_config_path + "/" + config_file_name
flag = zoo.get_config(zk_config_node, local_config_name)
if flag is False:
    sys.exit()

out = subprocess.Popen(["python", "./complex_pick.py ", "-c", local_config_name], stdout=subprocess.PIPE)

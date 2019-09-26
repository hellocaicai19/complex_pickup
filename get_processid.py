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
#zk_nodelist = "10.255.224.98:2181"
#zk_process_id_path = "/nonzc/cm/pick"
#zk_config_node = "/nonzc/cm/pick/config_pick_cm.ini"
#local_config_path = "/run/config"

zk_nodelist = os.environ.get("ZK_NODE_LIST")
zk_process_id_path = os.environ.get("ZK_PID_PATH")
zk_config_node = os.environ.get("ZK_CFG_NODE")
local_config_path = os.environ.get("LOCAL_CFG_PATH")

zoo = Zookeeper(zk_nodelist, None)
process_id = zoo.get_node(zk_process_id_path)
process_id = ''.join(process_id.split('_')[1:])
config_file_name = process_id + "_" + os.path.basename(zk_config_node)
local_config_name = local_config_path + "/" + config_file_name
flag = zoo.get_config(zk_config_node, local_config_name)
if flag is False:
    sys.exit()

out = subprocess.Popen(["python3", "./complex_pick.py", "-c", local_config_name]) # , stdout=subprocess.PIPE)
out.wait()

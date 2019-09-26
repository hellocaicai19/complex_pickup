# coding: utf-8
import time
import sys
import os
import getopt
import logging
from config import Config
from lib.nframe import PublicLib
from zookeeper import Zookeeper
from zk_redo import ZkRedo
from lib.receive_signal import ReceiveSignal

ReceiveSignal.receive_signal()

pl = PublicLib()
config_file = ''
# 获取用户传入的参数
try:
    opts, args = getopt.getopt(sys.argv[1:], 'c')
    opt = opts[0][0]
    if opt == '-c':
        config_file = args[0]
    else:
        print('-c:  configfile')
        sys.exit()
except getopt.GetoptError as e:
    print(e)
    sys.exit()
if not os.path.isfile(config_file):
    print("config file :%s not exist,exit" % config_file)
    sys.exit()

# 创建配置文件实例,获取配置文件内容
process_id = os.path.basename(config_file).split("_")[0]
config = Config(config_file)
cfg = config.get_config()
log_path = cfg["common"]["logpath"].strip()
zk_host_list = os.environ.get("ZK_NODE_LIST") 
zk_process_path = os.environ.get("ZK_PID_PATH")
bak_path = cfg["common"]["bakpath"].strip()
input_dir = cfg["common"]["inputdir"].strip()
up_path = cfg["common"]["uppath"].strip()

#校验配置文件参数是否合法
if log_path == "":
    print("log path is null! please check the config file, exit")
    sys.exit()
if input_dir == "":
    print("input path is null! please check the config file, exit")
    sys.exit()
if zk_process_path == "":
    print("zk process path is null! please check the config file, exit")
    sys.exit()
if zk_host_list == "":
    print("zk host list is null! please check the config file, exit")
    sys.exit()

#校验目录存在
if not os.path.exists(log_path):
    print("log path:%s not exist, please check the path, exit" % log_path)
    sys.exit()
if bak_path != "" and  not os.path.exists(bak_path):
    print("bak path:%s not exist, please check the path, exit" % bak_path)
    sys.exit()
if not os.path.exists(input_dir):
    print("input_dir:%s not exist, please check the path, exit" % input_dir)
    sys.exit()

# 创建zookeeper实例
zoo = Zookeeper(zk_host_list, None)
zoo.connect()
work_node = "process_" + process_id
pl.set_log(log_path, process_id)
#创建flow
flow = config.create_flow(process_id)

从zk获取redo
redo_node = zk_process_path + "/" + work_node + "/" + "redo"
redo_node_flag = zoo.check_exists(redo_node)
if redo_node_flag is not None:
    redo_info, stat = zoo.get_node_value(redo_node)
    redo_info = bytes.decode(redo_info)
    if redo_info is not None:
        output_dirs = config.output_dirs
        logging.info("get redo info from zk:%s" % redo_info)
        zk_redo = ZkRedo(redo_info, process_id, input_dir, output_dirs, bak_path, up_path)
        zk_redo.do_task()
    zoo.delete_node(redo_node)
    logging.info("redo_task end")

while 1:
    flow = config.create_flow(process_id)
    file_num = config.get_file()
    if file_num == 0:
        logging.info("no file in input dir,wait")
        time.sleep(5)
        if ReceiveSignal.EXIT_FLAG:
            sys.exit()
        continue
    flow.work(zoo, redo_node)
    if ReceiveSignal.EXIT_FLAG:
        logger.info("get exit signal,exit")
        sys.exit()

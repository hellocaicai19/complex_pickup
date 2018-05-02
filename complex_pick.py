# coding: utf-8
import time
import logging
import sys
import os
import getopt
from config import Config
from lib.nframe import PublicLib
from zookeeper import Zookeeper

from zk_redo import ZkRedo

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
    print("config file :%s not exsit" % config_file)
    sys.exit()
# 创建配置文件实例,获取配置文件内容
config = Config(config_file)
cfg = config.get_config()
log_path = cfg["common"]["logpath"].strip()
zk_process_path = cfg["zookeeper"]["processpath"].strip()
zk_host_list = cfg["zookeeper"]["zklist"].strip()
bak_path = cfg["common"]["bakpath"].strip()
input_dir = cfg["common"]["inputdir"].strip()

if log_path == "":
    logging.error("log path is null! please check the config file")
    sys.exit()
if input_dir == "":
    logging.error("input path is null! please check the config file")
    sys.exit()
if zk_process_path == "":
    logging.error("zk process path is null! please check the config file")
    sys.exit()
if zk_host_list == "":
    logging.error("zk host list is null! please check the config file")
    sys.exit()

if not os.path.exists(log_path):
    logging.info("logpath:%s not exist, please check the config file" % log_path)
    sys.exit()
if not os.path.exists(bak_path):
    logging.error("bak_path:%s not exist, please check the config file" % log_path)
    sys.exit()
if not os.path.exists(bak_path):
    logging.error("bak_path:%s not exist, please check the config file" % log_path)
    sys.exit()

# 创建zookeeper实例
zoo = Zookeeper(zk_host_list, None)
work_node = zoo.get_node(zk_process_path)
process_id = ''.join(work_node.split('_')[1:])
pl.set_log(log_path, process_id)
flow = config.create_flow(process_id)
redo_node = zk_process_path + "/" + work_node + "/" + "redo"
redo_node_flag = zoo.check_exists(redo_node)

if redo_node_flag is not None:
    redo_info, stat = zoo.get_node_value(redo_node)
    if redo_info is not None:
        output_dirs = config.output_dirs
        logging.info("redo info %s" % redo_info)
        zk_redo = ZkRedo(redo_info, process_id, input_dir, output_dirs, bak_path)
        zk_redo.do_task()
    zoo.delete_node(redo_node)

# redo_file = cfg["common"]["redopath"] + "/" + business_name + "_pick." + process_id + ".redo"
# check_redo = CheckRedo(redo_file, process_id, config.output_dirs)
# recover = check_redo.do_task()
# if recover == 1:
#     logging.info('redo:recover=1,Revert...')
#     flow.work(zoo, redo_node)

while 1:
    flow = config.create_flow(process_id)
    file_num = config.get_file()
    if file_num == 0:
        logging.info("no file in input dir,wait")
        time.sleep(5)
        continue
    flow.work(zoo, redo_node)

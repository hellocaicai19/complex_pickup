# coding: utf-8
import time
import logging
import datetime
import sys
import os
import getopt
from config import Config
from lib.nframe import PublicLib
from zookeeper import Zookeeper
from check_redo import CheckRedo
from pick_file import PickFile

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
log_path = cfg["common"]["logpath"]
if not os.path.exists(log_path):
    logging.info("logpath:%s not exist" % log_path)
    sys.exit()
process_path = cfg["zookeeper"]["processpath"]
zk_host_list = cfg["zookeeper"]["zklist"]
# 创建zookeeper实例
zoo = Zookeeper(zk_host_list, None)
process_id = zoo.get_node(process_path)
pl.set_log(log_path, process_id)
flow = config.create_flow(process_id)
business_name = cfg["common"]["business"]

redo_file = cfg["common"]["redopath"] + "/" + business_name + "_pick." + process_id + ".redo"
check_redo = CheckRedo(redo_file, process_id, config.output_dirs)
recover = check_redo.do_task()
if recover == 1:
    logging.info('redo:recover=1,Revert...')
    flow.work()
while 1:
    # 获取当前系统序号
    flow = config.create_flow(process_id)
    file_num = config.get_file()
    if file_num == 0:
        logging.info("no file in input dir..")
        time.sleep(5)
        continue
    flow.work()
    logging.info("batch work end")

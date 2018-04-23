import os
import re
import sys
import logging
# coding: utf-8


class CheckRedo:

    """
    检查redo信息
    """
    def __init__(self, redo_file, process_id, output_dirs):
        self.redo_file = redo_file
        self.process_id = process_id
        self.output_dirs = output_dirs
        self.filename = ""
        self.action_step = ""
        self.output_dir = ""
        self.file_list = ""

    def read_redo(self):
        """
        读redo文件
        :return:
            -1:不存在redo文件
            "":redo信息不完整
            "PICKUP":在分拣过程中进程停止
            "END"：在分拣后-->文件挪走过程中停掉

             
        """
        if not os.path.exists(self.redo_file):
            logging.info("no redo file")
            return -1
        logging.info("get redo file:%s" % self.redo_file)
        with open(self.redo_file) as file:
            p_fn = re.compile(r"^todo_list = ")
            p_as = re.compile(r"^action_step = ")
            p_fl = re.compile(r"^file_list = ")
            for line in file:
                if re.findall(p_fn, line):
                    self.filename = (line[12:])
                    continue
                if re.findall(p_as, line):
                    self.action_step = (line[14:])
                    continue
                if re.findall(p_fl, line):
                    self.file_list = (line[12:])
                    continue
        return self.action_step.strip("\n").strip()

    def move_pickfile(self, remove=False):
        """
        :param remove:
        """
        recover = 0

        if remove:
            for key, value in self.output_dirs.items():
                files = os.listdir(value + "/" + self.process_id)
                if not files:
                    continue
                filename = self.filename.replace("\n", "").strip()
                p1 = re.compile(filename)
                for file in files:
                    if re.findall(p1, file):
                        try:
                            os.rename(value + "/" + self.process_id + "/" + file, value + "/" + file)
                            logging.info("REDO: MOVE FILE:%s to %s" % (value + "/" + self.process_id + "/" + file, value
                                                                       + "/" + file))
                        except Exception as e:
                            logging.error("move file err:%s" % e)
                            sys.exit()
        else:
            for key, value in self.output_dirs.items():
                if not os.path.isdir(value):
                    logging.error("ERROR:%s not exist" % value)
                    sys.exit()

                files = os.listdir(value + '/' + self.process_id)
                filename = self.filename.replace("\n", "").strip()
                p1 = re.compile(filename)
                for file in files:
                    if re.findall(p1, file):
                        os.remove(value + "/" + self.process_id + "/" + file)
                        logging.info("REDO REMOVE FILE:%s" % (value + "/" + self.process_id + "/" + file))
            recover = 1
        return recover

    def do_task(self):
        """

        :return: 0 1).没有redo文件，主程序正常往下执行
                   2).回滚完毕，主程序正常往下执行
                 1 入口文件没有处理完，需要重新处理，此时不需要再从源目录find文件，redo中需要记录seq
        """
        flag = 0
        action_step = self.read_redo()
        if action_step == -1:
            return flag
        if action_step == "PICKUP":
            flag = self.move_pickfile()
        elif action_step == "END":
            flag = self.move_pickfile(True)
        os.remove(self.redo_file)
        return flag

import os
import re
import sys
import logging
# coding: utf-8


class ZkRedo:

    """
    检查redo信息
    """
    def __init__(self, redo_info, process_id, input_dir, output_dirs, bak_path, up_path):
        self.redo_info = redo_info
        self.process_id = process_id
        self.input_dir = input_dir
        self.output_dirs = output_dirs
        self.bak_path = bak_path
        self.up_path = up_path

    def read_redo(self):
        redo_info_list = self.redo_info.split(";")
        step = 0
        if "begin" in redo_info_list:
            step = 1
        if "end" in redo_info_list:
            step = 2
        return step

    def move_pickfile(self, remove=False):
        """
        :param remove:
        """
        if remove:
            # 将已经分拣出来的文件删掉
            for rule, output_dir in self.output_dirs.items():
                output_temp = os.path.join(output_dir, self.process_id)
                files = os.listdir(output_temp)
                if not files:
                    continue
                for file in files:
                    if not os.path.isfile(file):
                        continue
                    try:
                        file_path = os.path.join(output_temp, file)
                        os.remove(file_path)
                        logging.info("redo work:delete invalid file success :%s " % file_path)
                    except Exception as e:
                        logging.error("redo work:delete invalid file failed, err:%s" % e)
                        sys.exit()
            return
        logging.info("test:output_dirs:%s" % self.output_dirs)
        for rule, output_dir in self.output_dirs.items():
            output_temp = os.path.join(output_dir, self.process_id)
            ofn_list = os.listdir(output_temp)
            if not ofn_list:
                continue
            for ofn in ofn_list:
                file_path = os.path.join(output_temp, ofn)
                if not os.path.isfile(file_path):
                    continue
                try:
                    new_path = os.path.join(output_dir, ofn)
                    os.rename(file_path, new_path)
                    logging.info("redo work:move output file success,%s to %s" % (file_path, new_path))
                except Exception as e:
                    logging.error("redo work:move output file failed, err:%s" % e)
                    sys.exit()
        input_temp = os.path.join(self.input_dir, self.process_id)
        ifn_list = os.listdir(input_temp)
        if not ifn_list:
            return
        for ifn in ifn_list:
            file_path = os.path.join(input_temp, ifn)
            if not os.path.isfile(file_path):
                continue
            try:
                if self.bak_path == "":
                    os.remove(file_path)
                    logging.info("redo work:remove input file %s success " % file_path)
                    continue
                new_path = os.path.join(self.bak_path, ifn)
                os.rename(file_path, new_path)
                logging.info("redo work:move file success,%s to %s" % (file_path, new_path))
            except Exception as e:
                logging.error("redo work:move file failed, err:%s" % e)
                sys.exit()
        if self.up_path != "":
            up_temp_path = os.path.join(self.up_path, self.process_id)
            up_files = os.listdir(up_temp_path)
            for up_file in up_files:
                if not os.path.isfile(up_file):
                    continue
                sf = os.path.join(up_temp_path, up_file)
                tf = os.path.join(self.up_path, up_file)
                try:
                    os.rename(sf, tf)
                    logging.info("redo work: move upfile success,%s to %s" % (sf, tf))
                except Exception as e:
                    logging.error("redo work:move upfile failed, err:%s" % e)
                    sys.exit()
        return

    def do_task(self):

        action_step = self.read_redo()
        if action_step == 1:
            self.move_pickfile(True)
        elif action_step == 2:
            self.move_pickfile()

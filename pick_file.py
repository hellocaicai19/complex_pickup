import os
import re
import logging
# coding: utf-8


class PickFile:

    def __init__(self, input_dir, process_input_dir, match_expr, batch_size):
        self.input_dir = input_dir
        self.process_input_dir = process_input_dir
        self.match_expr = match_expr
        self.batch_size = batch_size

    def move_file(self):
        """
        将此业务的文件挪入相应的入口临时目录

        """
        if not(os.path.exists(self.process_input_dir)):
            os.mkdir(self.process_input_dir)

        files = os.listdir(self.input_dir)
        file_num = 0
        for file in files:
            source_file = os.path.join(self.input_dir, file)
            if (not (os.path.exists(source_file))) or (not (os.path.isfile(source_file))):
                continue

            p1 = re.compile(self.match_expr)
            if re.findall(p1, file):
                try:
                    os.rename(source_file, os.path.join(self.process_input_dir, file))
                    logging.info('BEGIN:MOVE %s TO %s/%s' % (file, self.process_input_dir, file))
                except FileNotFoundError:
                    logging.info("file %s not found, continue" % source_file)
                    continue

            file_num += 1
            if file_num >= int(self.batch_size):  # need to configure the file_batch_size in config file
                break
        return file_num

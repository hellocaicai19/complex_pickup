import os
import sys
import logging
# coding: utf-8


class XdrFile:
    def __init__(self):

        self.file = None
        self.headers = []
        self.contents = []
        self.tails = []
        self.file_name = ""

    def open_xdr_file(self, filename, flag):

        self.file_name = filename
        self.file = open(filename, flag)

    def get_filename(self):

        return self.file_name

    def read_xdr_file(self, fieldlen):

        self.read_header()
        self.read_content(fieldlen)

    def write_xdr_file(self, content):
        try:
            self.file.writelines(content)
        except IOError as e:
            logging.error(e)
            sys.exit()

    def close_xdr_file(self):

        self.file.close()

    def read_header(self):
        '''
        读取文件头
        '''
        header_num = 0
        for line in self.file:
            line = line.strip(os.linesep)
            header_num += 1
            self.headers.append(line)
            if header_num == 6:
                break

    def read_content(self, fieldlen):
        '''
        读取文件内容，并存入数组
        '''
        for line in self.file:
            # line = line.strip(os.linesep)
            items = line.split(";")
            items = items[2:]   #去前两列
            # yield items

            if len(items) != int(fieldlen):  #校验字段长度
                logging.error("There is an incomplete line,len:%s line:%s" % (len(items), items))
                sys.exit()
            self.contents.append(items)   #二维数组
        self.file.close()   

    def get_contents(self):
        return self.contents

    def set_contents(self, contents):
        self.contents = []
        self.contents = contents

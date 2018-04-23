#!/usr/bin/env python3

# encoding: utf-8

'''

@author: lipd

@file: changefile

@time: 2018/3/9 10:02

@desc:
    1.判断文件是否带头，如果有头则去掉文件头
    2.去掉话单前两列

'''

import os
import sys
import time

input_path = "f:/sort/sort_stream/bak"
output_path = "f:/sort/sort_stream/upfile"

if not os.path.exists(input_path) or input_path == "":
    print("inputpath:%s not exists" % input_path)
    sys.exit()

if not os.path.exists(output_path) or output_path == "":
    print("outputpath:%s not exists" % input_path)
    sys.exit()

input_files = os.listdir(input_path)
while 1:
    for file in input_files:
        input_file = input_path + "/" + file
        if not os.path.isfile(input_file):
            continue
        print("begin-->%s" % input_file)
        file_content = []
        source_file = open(input_file)
        for line in source_file:
            line_list = line.split(";")
            if len(line_list) <= 1:
                continue

            new_line = ";".join(line_list[2:])
            file_content.append(new_line)
        print(len(file_content))
        file = open(output_path + "/" + file, "a")
        file.writelines(file_content)
        file.close()
        print("done-->%s" % input_file)
    break
print("end....")

#!/usr/bin/env python3

# encoding: utf-8

'''

@author: lipd

@file: test_read.py

@time: 2018/4/16 9:52

@desc:

'''
import os

file_list = os.listdir("f:/asiainfo")
file = open("f:/test1", "a")
file.writeline(file_list)
file.close()

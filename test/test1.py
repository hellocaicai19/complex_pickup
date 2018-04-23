#!/usr/bin/env python3

# encoding: utf-8

'''

@author: lipd

@file: test1.py

@time: 2018/4/10 10:08

@desc:

'''
import re
#
#
# str = "CHARGE4!=0 and SP_BAL_PROV==(100,200,210,220,230,240,250,270,280,290,311,351,371,431,451,471,531,551,571,591,731,771,791,851,871,891,898,931,951,991,971)"
#
# prov_list = re.findall('\((.*?)\\)', str)
# str_list = str.split("(")
# print(str.split("(")[0])
# print(type(prov_list))
#
# if "(" in str:
#     print("hello")

config = "HEAD,DATE,PROV,SPLIT,SEQ"
config_list = config.split(",")
HEAD = "B"
DATE = "20180101"
PROV = "200"
SPLIT = "."
SEQ = "001"
filename = ""
for c in config_list:
    filename += locals()[c]

print(filename)

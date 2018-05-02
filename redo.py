#!/usr/bin/python
# -*- coding:utf-8 -*-
import logging
'''
    @Info:
        继承内容计费接口基类，实现并提供write_redo和delete_redo方法，
        目前只支持disc-file-redo和zookeeper-redo两种方式
    @Author: zangxl
    @Date:20170315
    @Version: v1.0.0
'''
import BaseInterfaces
import os


class Redo(BaseInterfaces.BaseInterfaces):
    '''
        @Info:
            删除redo方法
        @Args:
            1、redo文件所在路径，建议写绝对路径
            2、是否记录Zookeeper-redo开关
            3、连接Zookeeper的host-list
            4、Zookeeper-redo文件
        @Return:
            0：redo删除成功
        @Raise：
            IOError
            ZKError
    '''

    def delete_redo(self, redo_file, on_zk=False, zk_host_list=None, zk_redo_file=None):

        if os.path.exists(redo_file) and os.path.getsize(redo_file) != 0:
            logging.info("[INFO] >>> remove redo file: %s" % redo_file)
            os.remove(redo_file)

        else:
            # raise IOError("[WARN] >>>redo file is not exist or redo_file size is 0!!!<<<")
            logging.info("[WARN] >>>redo file is not exist or redo_file size is 0!!!<<<")
        '''
        if on_zk is True:
            try:
                zk = zkClient.zkClient(zk_host_list)
                zk.delete(zk_redo_file)

            except Exception as e:
                raise Exception("[ERROR] >>>zk delete redo file faild!!!<<< ERROR_INFO: %s" % str(e))

            finally:
                zk.close()
        '''
        return 0

    '''
        @Info:
            写redo方法
        @Args:
            1、redo文件所在路径，建议写绝对路径
            2、redo信息，字典类型
            3、Zookeeper对象，默认为空
            4、zk-redo文件，默认为空
        @Return:
            1:
                i.  redo文件不存在
                ii. redo文件不为空
            0: redo写入成功
        @Raise：
            ZkException
    '''
    def write_redo(self, redo_file, redo_content_dic, on_zk=False, zk_host_list=None, zk_redo_file=None):
        if not os.path.exists(redo_file):
            logging.info("[WARN] >>>no redo file defined, can not write data to redo file<<<")

            return 1

        redo_f = open(redo_file, "a")

        if len(redo_content_dic) == 0:
            logging.info("[WARN] >>>redo content is null, please to recover redo-file first<<<")

            return 1

        for key in redo_content_dic.keys():
            # print (key)
            value = redo_content_dic.get(key)
            # redo_msg = ",".join(value)
            redo_msg = "%s = %s \n" % (key, value)
            redo_f.write(redo_msg)
            """
            if on_zk is True:
                try:
                    zk = zkClient.zkClient(zk_host_list)
                    zk.save(zk_redo_file, redo_msg)

                except Exception as e:
                    raise Exception("[ERROR] >>>write zk redo file failed!!!<<< ERROR_INFO: %s" % str(e))

                finally:
                    zk.close()
            """
        redo_f.close()
        return 0

import os
import sys
import logging
from xdr_file import XdrFile
import re

# coding: utf-8


class Rule:
    def __init__(self, config):
        self.config = config
        self.fields = self.create_fields(config)
        self.xdrFile = None
        self.contents = []
        self.allContents = []
        self.outputlist = []
        self.process_file_dir = ""
        self.process_id = ""

    def get_outputlist(self):
        return self.outputlist

    @staticmethod
    def create_fields(config):
        """
        将配置文件中的fieldname fieldindex fieldtype放到字典fields里面
        :param config:
        :return:
        """
        fields = {}
        try:
            field_name = config['fieldname'].split(',')    # 字段名称
            field_index = config['fieldindex'].split(',')  # 字段在话单中的位置
            field_type = config['fieldtype'].split(',')    # 字段类型（int,string）
        except keyError as e:
            logging.error('The configuration item is incomplete %s' % e)
            sys.exit()
       
        if len(field_index) == len(field_name) == len(field_type):
            for i in range(len(field_name)):
                if field_index[i] != "":
                    fields[field_name[i]] = (field_type[i], int(field_index[i])-1)
        else:
            fields = None
        return fields

    def get_fields(self):
        return self.fields

    def work(self, xdr_file, cur, output_filename, source_filename):
        output_list = []
        filename_part = self.config.get("filenamepart")
        condition_expr = self.config.get("conditionexpr")
        dest_filename = self.config.get("destfilename")
        group_fields = self.config.get("groupfield")
        condition_mutex = self.config.get("conditionmutex")
        dest_dir = self.config.get("destdir")
        need_null = self.config.get("neednulldestfile")
        SFN = source_filename
        OFN = output_filename
        HEAD = dest_filename
        SPLIT = "."
        of_temp_dir = dest_dir + "/" + self.process_id + "/"
        if not os.path.exists(of_temp_dir):
            os.mkdir(of_temp_dir)
            logging.info("%s not exist, make it" % of_temp_dir)
        out_filename = ""
        self.xdrFile = xdr_file
        str_sql = "SELECT id FROM sorttable%s " % self.process_id
        if condition_expr != "":  # 判断该rule是否有查询条件
            # 按省查询
            if "{" in condition_expr:  # 判断是否是按省查询
                prov_list = re.findall('\{(.*?)\\}', condition_expr)  # 截取出省代码列表
                factor = condition_expr.split("{")[0]
                # 被该分拣条件选中的话单不再被其他分拣条件分拣
                if condition_mutex == "true":
                    for prov in prov_list[0].split(","):
                        str_sql = "SELECT id FROM sorttable%s " % self.process_id
                        str_sql = str_sql + " WHERE " + factor + prov
                        cur.execute(str_sql)
                        contents = cur.fetchall()
                        if not contents:
                                continue
                        filename = ""
                        PROV = prov
                        filename_part_list = filename_part.split(",")
                        for part in filename_part_list:
                            if part.startswith("$"):
                                part = part.strip("$")
                                filename += locals()[part]
                            else:
                                filename += part
                        out_filename = of_temp_dir + filename
                        self.do_to_onefile(out_filename, contents)
                        output_list.append(out_filename)
                        # 从表中删除符合该分拣条件的数据
                        delete_sql = "DELETE FROM sorttable" + self.process_id + " WHERE + factor + prov"
                        cur.execute(delete_sql)
                # 被该分拣条件选中的话单可以被其他分拣条件分拣
                else:
                    for prov in prov_list[0].split(","):
                        str_sql = "SELECT id FROM sorttable%s " % self.process_id
                        str_sql = str_sql + " WHERE " + factor + prov
                        cur.execute(str_sql)
                        contents = cur.fetchall()
                        if not contents:
                            continue
                        filename = ""
                        PROV = prov
                        filename_part_list = filename_part.split(",")
                        for part in filename_part_list:
                            if part.startswith("$"):
                                part = part.strip("$")
                                filename += locals()[part]
                            else:
                                filename += part
                        out_filename = of_temp_dir + filename
                        self.do_to_onefile(out_filename, contents)
                        # for content in contents:
                        #     str_sql_update = "UPDATE sorttable set FLAG=1 where id = %s" % content[0]
                        #     try:
                        #         cur.execute(str_sql_update)
                        #     except Exception as e:
                        #         logging.error("update error:%s" % e)
                        content_0_list = []
                        for content in contents:
                            content_0_list.append(str(content[0]))
                        if len(content_0_list) > 0:
                            content_0_str = ",".join(content_0_list)
                            str_sql_update = "UPDATE sorttable" + self.process_id + \
                                             " set FLAG=1 where id in (" + content_0_str + ")"
                            try:
                                cur.execute(str_sql_update)
                            except Exception as e:
                                logging.error("update error:%s" % e)
                                sys.exit()
                        output_list.append(out_filename)

                return output_list
            # 不按省查询
            else:
                filename = ""
                filename_part_list = filename_part.split(",")
                for part in filename_part_list:
                    if part.startswith("$"):
                        part = part.strip("$")
                        filename += locals()[part]
                    else:
                        filename += part
                out_filename = of_temp_dir + filename
                str_sql = str_sql + " WHERE " + condition_expr
                try:
                    cur.execute(str_sql)
                except Exception as e:
                    logging.error("execute sql err: %s" % e)
                    sys.exit()
                contents = cur.fetchall()
                if not contents:
                    if need_null == "0":
                        return
                if group_fields == "" or group_fields is None:

                    self.do_to_onefile(out_filename, contents)
                    output_list.append(out_filename)
                else:
                    self.do_to_onefile(out_filename, contents)
                    output_list.append(out_filename)
                if condition_mutex == "true":
                    try:
                        cur.execute("DELETE FROM sorttable%s WHERE %s" % (self.process_id, condition_expr))
                    except Exception as e:
                        logging.error("execute sql err: %s" % e)
                        sys.exit()
                return output_list
        # 该rule无查询条件
        else:
            str_sql = " SELECT id FROM sorttable%s where FLAG == '' " % self.process_id
            try:
                cur.execute(str_sql)
            except Exception as e:
                logging.error("execute sql err: %s" % e)
                sys.exit()
            contents = cur.fetchall()
            if not contents:
                if condition_mutex == "true":
                    try:
                        cur.execute("DELETE FROM sorttable%s" % self.process_id)
                        logging.info("DELETE FROM sorttable%s" % self.process_id)
                    except Exception as e:
                        logging.error("execute sql err: %s" % e)
                        sys.exit()
                return
            filename = ""
            filename_part_list = filename_part.split(",")
            for part in filename_part_list:
                if part.startswith("$"):
                    part = part.strip("$")
                    filename += locals()[part]
                else:
                    filename += part
            out_filename = of_temp_dir + filename
            if group_fields == "" or group_fields is None:
                self.do_to_onefile(out_filename, contents)
                output_list.append(out_filename)
            else:
                self.do_to_onefile(out_filename, contents)
                output_list.append(out_filename)
            if condition_mutex == "true":
                    try:
                        cur.execute("DELETE FROM sorttable%s" % self.process_id)
                        logging.info("DELETE FROM sorttable%s" % self.process_id)
                    except Exception as e:
                        logging.error("execute sql err: %s" % e)
                        sys.exit()
        return output_list

    def do_to_onefile(self, ofn, contents, xdr_file=None):
        """
        写入文件
        :param ofn: 输出文件名
        :param contents: 文件内容
        :param xdr_file:
        :return:
        """

        file_content = []
        ori_file_content = self.xdrFile.get_contents()
        if xdr_file is None and self.xdrFile is None:
            sys.exit()
        elif xdr_file:
            ori_file_content = xdr_file.get_contents()
        elif self.xdrFile:
            ori_file_content = self.xdrFile.get_contents()
        if contents:
            for content in contents:
                # try:
                #     ori_file_content[content[0]][130] = os.path.basename(ofn)
                #     line = ";".join(ori_file_content[content[0]])  # + "\n"
                # except IndexError as e:
                #     logging.error("change xdr's filename err:%s" % e)
                #     sys.exit()
                try:
                    line = ";".join(ori_file_content[content[0]])
                    file_content.append(line)
                except Exception as e:
                    logging.error("build  xdr err:%s" % e)
                    sys.exit()
        file_len = len(file_content)
        out_xdr_file = XdrFile()
        out_xdr_file.open_xdr_file(ofn, 'a')
        out_xdr_file.write_xdr_file(file_content)
        out_xdr_file.close_xdr_file()
        logging.info("write file:%s,line num:%d" % (ofn, file_len))

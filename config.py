import os
import sys
import logging
from configparser import ConfigParser
from flow import Flow
from rule import Rule
from pick_file import PickFile
from check_redo import CheckRedo
from verify import Verify
# coding: utf-8


class Config:
    def __init__(self, filepath):
        self.configPath = ""
        self.process_input_dir = ""
        self.input_dir = ""
        self.match_expr = ""
        self.parser = ConfigParser()
        if os.path.isfile(filepath):
            self.config_file = filepath
        self.batch_size = 5
        self.output_dirs = {}

    def get_config(self):
        """
        获取配置文件信息
        :return:config
        """
        parser = ConfigParser()
        parser.read(self.config_file)
        sections = []
        config = {}
        for section in parser.sections():
            sections.append(section)
            items = {}
            for key, value in parser.items(section):
                items[key] = value
                config[section] = items
        return config

    def get_file(self):
        pickfile = PickFile(self.input_dir, self.process_input_dir, self.match_expr, self.batch_size)
        file_num = pickfile.move_file()
        return file_num

    def create_flow(self, process_id):
        flow = Flow()  # 初始化一个flow实例
        config = self.get_config()

        # 检查配置项是否存在
        if not config.get("common")["inputdir"]:
            logging.error("ERROR>>no inputdir in %s<< " % self.config_file)
            sys.exit()
        self.input_dir = config.get("common")["inputdir"]

        if not config.get("common")["input_rule_exp"]:
            logging.error("ERROR>>no input_rule_exp in %s<< " % self.config_file)
            sys.exit()

        self.match_expr = config.get("common")["input_rule_exp"]

        if not config.get("common")["redopath"]:
            logging.error("ERROR>>no redopath in %s<< " % self.config_file)
            sys.exit()
        redo_path = config.get("common")["redopath"]

        if not config.get("common")["fieldlen"]:
            logging.error("ERROR>>no fieldlen in %s<< " % self.config_file)
            sys.exit()
        fieldlen = config.get("common")["fieldlen"]

        if not config.get("common")["line_limit"]:
            logging.error("ERROR>>no line_limit in %s<< " % self.config_file)
            sys.exit()
        line_limit = config.get("common")["line_limit"]
        if line_limit == "":
            line_limit = 20000
        if not config.get("common")["rules"]:
            logging.error('ERROR>>no rules in config<<')
            sys.exit()
        rule_list = config.get("common")["rules"].split(",")

        self.batch_size = config.get("common")["batchsize"]

        if not config.get("common")["bakpath"]:
            logging.error('ERROR>>no bakpath in config<<')
            sys.exit()
        bak_path = config.get("common")["bakpath"]

        flow.set_fieldlen(fieldlen)
        flow.set_line_limit(int(line_limit))
        flow.set_process_id(process_id)
        flow.set_redo_path(redo_path)
        flow.set_bak(bak_path)

        output_dirs = {}
        # 检查各rule是否配置了输出目录
        for rule in rule_list:
            output_dir = config.get(rule)["destdir"]
            if output_dir == "":
                logging.error("rule:%s no destdir" % rule)
                sys.exit()
            output_dirs = {rule: output_dir}

        # 检查配置文件中的路径信息是否存在
        all_path = {'inputdir': self.input_dir, 'redopath': redo_path, 'bakpath': bak_path}
        all_path.update(output_dirs)
        self.output_dirs = all_path
        verify = Verify(all_path)
        if not verify.check_path():
            sys.exit()

        self.process_input_dir = self.input_dir + "/" + process_id
        flow.set_dir(self.process_input_dir)

        for rule_name in rule_list:
            _config = {'rulename': rule_name}
            rule_items = config.get(rule_name)
            _config.update(rule_items)
            flow.add_rule(Rule(_config))  # 返回一个fields
        flow.config = config
        return flow

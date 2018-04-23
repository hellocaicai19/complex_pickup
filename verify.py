import os
import logging


class Verify(object):
    """
        检查配置文件中的所有路径

    """

    def __init__(self, config_dict):

        self.std_err = None
        self.config_dict = config_dict

    def check_path(self):
        """
        遍历检查所有路径是否存在
        :return:
            True:所有路径均存在
            False:有路径不存在
        """
        for key, value in self.config_dict.items():
            if value:
                if not os.path.exists(value):
                    logging.error("the path: %s not exist!" % value)
                    return False
        return True



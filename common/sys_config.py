'''
加载sys_config.ini中的配置项sys
sys配置项用于存放程序运行时的环境参数
'''
import configparser
import os

conf_dict = configparser.ConfigParser()
conf_dict.read('%s/../configs/sys_config.ini' % os.path.dirname(__file__))
conf_dict = conf_dict['sys']
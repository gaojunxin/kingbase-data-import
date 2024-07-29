#!/usr/bin/python
# -*- coding: UTF-8 -*-
import datetime
import os
from db_tools import DBTools
from mergedeep import merge


import logging
import yaml
from excel_runner import ExcelRunner
from table_runner import TableRunner

dict_id_map = {}
dict_name_map = {}
# 根据目录加载配置文件
def load_yaml_files(directory):
    """Load all YAML files in a directory into a single dictionary."""
    config = {}
    for filename in os.listdir(directory):
        if filename.endswith('.yaml') or filename.endswith('.yml'):
            filepath = os.path.join(directory, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                try:
                    content = yaml.safe_load(file)
                    # Deeply merge content into config using mergedeep
                    config = merge(config, content)
                except yaml.YAMLError as exc:
                    print(f"Error in configuration file '{filename}': {exc}")
    return config

def get_logger():
    # 创建一个日志记录器
    logger = logging.getLogger('kingbase_data_import')
    
    # 设置日志级别
    logger.setLevel(logging.INFO)
    
    # 创建一个控制台处理器
    ch = logging.StreamHandler()
    
    # 定义日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 为处理器添加格式
    ch.setFormatter(formatter)
    
    # 为日志记录器添加处理器
    logger.addHandler(ch)
    
    return logger

# 根据表名查询字典表数据
def get_type_dict(db_config, table_name, id_field, name_field):
    dbtools = DBTools(None, db_config)
    sql = f'select {id_field}, {name_field} from "{dbtools.get_schema_name()}"."{table_name}"'
    with dbtools.get_connection() as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                type_dict_idmap = {}
                type_dict_namemap = {}
                for item in result :
                    type_dict_idmap[item[0]] = item[1]
                    type_dict_namemap[item[1]] = item[0]
                return [type_dict_idmap, type_dict_namemap]
            except Exception as e:
                logging.error(f"查询字典[{table_name}]数据失败", exc_info=True)

# 初始化字典表
def init_dict(database, dict_tables):
    for dict_info in dict_tables:
        table_name = dict_info['name']
        id_field = dict_info['id_field']
        name_field = dict_info['name_field']
        db = dict_info['db']
        data = get_type_dict(database[db], table_name, id_field, name_field)
        dict_id_map[table_name] = data[0]
        dict_name_map[table_name] = data[1]

if __name__ == "__main__":
    logger = get_logger()
    try:
        with open('config.yaml', 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            if os.path.exists('./tasks'):
                task_config = load_yaml_files('./tasks')
                config = merge(config, task_config)
    except Exception as e:
        logger.error(e)
        input("Press Enter to exit...")

    database = config['database']
    task_config = config['tasks']
    dict_tables = config['dict_tables']

    # 初始化字典映射
    init_dict(database, dict_tables)
    sorted_task_config = sorted(task_config, key=lambda task: task_config[task]['order'])

    for task_id in sorted_task_config:
        item_config = task_config[task_id] 
        if item_config['active'] == True:
            runner_type = item_config.get('type', 'excel')
            if runner_type == 'excel':
                runner = ExcelRunner(item_config, database, logger, dict_tables, dict_id_map, dict_name_map)
            elif runner_type == 'table':
                runner = TableRunner(item_config, database, logger, dict_tables, dict_id_map, dict_name_map)
            else:
                logger.error(f"不支持的runner类型[{runner_type}]")
                break
            runner.run(task_id)
    
    input("所有任务完成，输入回车键退出...")
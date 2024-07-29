#!/usr/bin/python
# -*- coding: UTF-8 -*-
import csv
import datetime
import os
import json
import string
import pandas as pd
from pandas import concat

from openpyxl import load_workbook
import logging
from io import StringIO

from db_tools import DBTools


class ExcelRunner:
    def __init__(self, task_config, database, logger, dict_tables, dict_id_map, dict_name_map) -> None:
        self.task_config = task_config
        self.database = database
        self.dict_name_map = dict_name_map
        self.dict_id_map = dict_id_map
        self.logger = logger
        self.dict_tables = dict_tables

        db = task_config['db']
        self.dbtools = DBTools(logger, database[db])

        self.file_path = task_config['excel_file']
        self.base_columns = task_config['base_column']
        self.default_column = task_config['default_column']

        self.base_table = task_config['base_table']
        self.extend_table = task_config['extend_table']
        self.current_id = 0
    def get_connection(self):
        return self.dbtools.get_connection()

    def remove_prefix(self, text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text
    
    def add_prefix(self, text, prefix):
        if text is not None and type(text) == 'str' and not text.startswith(prefix):
            return prefix+text
        return text
    # 将源表的数据清晰为目标表的格式
    def convert_sheet(self, sheet_name, df):

        base_column_names = []
        rname_config = {}
        id_vars = []
        ext_rename_config = {'value': 'field_value', 'variable': 'field_name'}
        # 基础字段名字从中文描述修改为数据库字段名
        for column in self.base_columns:
            name = column['name']
            index = column.get('index', None)
            if index is not None:
                caption = df.columns[index]
                base_column_names.append(name)
                rname_config[caption] = name

        df = df.rename(columns=rname_config)

        # 初始化基础字段
        for base_column_item in self.base_columns:
            mode = base_column_item.get('mode', '')
            col_name = base_column_item['name']
            to_col_name = base_column_item.get('to', '')
            from_column_name = base_column_item.get('from', '')
            dict_table = base_column_item.get('dict','')
            value_prefix = base_column_item.get('value_prefix',None)
            has_column = from_column_name in df.columns
            if mode is not None:
                if mode == 'type_code':
                    if sheet_name == '陆岸设施':
                        sheet_name = '陆岸终端'
                    elif sheet_name == '水下生产系统':
                        sheet_name = '水下生产装置'
                    elif sheet_name == '发电机组':
                        sheet_name = '电气设备'
                    df[col_name] = self.dict_name_map['epfaclas_term'][sheet_name]
                    base_column_names.append(col_name)
                elif mode == 'type_name':
                    df[col_name] = sheet_name
                    base_column_names.append(col_name)
                elif mode == 'id2name':
                    if has_column:
                        if value_prefix is not None:
                            df[col_name] = df[from_column_name].map(lambda x: self.remove_prefix(self.dict_id_map[dict_table].get(x, x), value_prefix))
                        else:
                            df[col_name] = df[from_column_name].map(self.dict_id_map[dict_table])
                        base_column_names.append(col_name)
                elif mode == 'name2id':
                    if has_column:
                        if value_prefix is not None:
                            df[col_name] = df[from_column_name].map(lambda x: self.add_prefix(self.dict_name_map[dict_table].get(x, x), value_prefix))
                        else:
                            df[col_name] = df[from_column_name].map(self.dict_name_map[dict_table])
                        base_column_names.append(col_name)
                elif mode == 'copy':
                    id_vars.append(col_name)
                    ext_rename_config[col_name] = to_col_name
                    base_column_names.append(col_name)

        # 处理默认字段
        for column_item in self.default_column:
            col_name = column_item['name']
            value = column_item['value']
            df[col_name] = value
            base_column_names.append(col_name)
        
        df_length = len(df)
        df['id'] = range(self.current_id + 1, self.current_id + df_length + 1)
        self.current_id += df_length

        # 截取基础字段
        base_datas = df[list(set(base_column_names))]

        # 截取扩展字段
        ext_columns = set(df.columns) - set(base_column_names) | set(id_vars)
        ext_datas = df[list(ext_columns)]

        self.logger.debug("基础字段：\n%s", base_datas)
        self.logger.debug("扩展字段：\n%s", ext_datas)
        
        # 使用 pd.melt() 将列转换为行
        value_vars = ext_datas.columns.difference(id_vars)
        melted_df = pd.melt(ext_datas, id_vars=id_vars, value_vars=value_vars)

        melted_df = melted_df.rename(columns=ext_rename_config)

        self.logger.debug("列转行后的结果：\n%s", melted_df)
        return base_datas, melted_df
    # 执行任务
    def run(self, task_id):
        self.logger.info('正在执行：%s', self.task_config.get('title', task_id))

        # 读取 Excel 文件
        all_sheets_df = pd.read_excel(self.file_path, sheet_name=None, engine='openpyxl')
        all_base_datas = []
        all_ext_datas = []
        # 遍历所有 sheet
        for sheet_name, df in all_sheets_df.items():
            self.logger.info('正在处理：%s', sheet_name)
            [base_datas, ext_datas]  = self.convert_sheet(sheet_name, df)
            all_base_datas.append(base_datas)
            all_ext_datas.append(ext_datas)
       
        final_base_datas = concat(all_base_datas)
        final_ext_datas = concat(all_ext_datas)
        final_ext_datas['id'] = range(1, len(final_ext_datas) + 1)
        final_ext_datas['del_flag'] = 0

        self.logger.info("数据梳理完毕，等待写入数据库...")
        self.logger.debug("基础字段表：\n%s", final_base_datas)
        self.logger.debug("扩展字段表：\n%s", final_ext_datas)

        self.logger.info("正在清空表...")
        clear_tables = self.task_config.get('clear_table', None)
        if clear_tables is not None:
            for table_item in clear_tables:
                table_name = table_item.get('name', None)
                if table_name is not None:
                    self.dbtools.truncate_table(table_name)
                    self.logger.info(f"清空表{table_name}完成")
        self.logger.info("所有表清空完成")


        engine = self.dbtools.create_engine()
        schema_name = self.dbtools.get_schema_name()
        final_base_datas.to_sql(name=f'{self.base_table}', schema=schema_name, if_exists="append", con=engine, index=False)
        self.logger.info(f"写入[基础字段表]: {self.base_table} 数据完成")

        final_ext_datas.to_sql(name=f'{self.extend_table}', schema=schema_name, if_exists="append", con=engine, index=False)
        self.logger.info(f"写入[扩展字段表]: {self.extend_table} 数据完成")
            
        self.logger.info('任务[%s]处理完成!', self.task_config.get('title', task_id))
        self.logger.info("\n---\n")

      
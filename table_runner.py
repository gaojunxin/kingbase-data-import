import os
import pandas as pd
import yaml
import logging
from pandas import concat
from datetime import datetime

from db_tools import DBTools

class TableRunner:
    def __init__(self, task_config, database, logger, dict_tables, dict_id_map, dict_name_map) -> None:
        self.database = database
        self.dict_name_map = dict_name_map
        self.dict_id_map = dict_id_map
        self.dbtools = None
        self.source_dbtools = None
        self.tables_config_to_write = {}
        self.logger = logger
        self.generate_column = False
        self.dict_tables = dict_tables
        self.task_config = task_config

        db = task_config['db']
        source_db = task_config['source_db']
        self.dbtools = DBTools(logger, database[db])
        self.source_dbtools = DBTools(logger, database[source_db])
        
         # 读取数据库列配置自动生成yaml中的配置，在打印日志中，需要自己拷贝到配置中，然后进行调整
        self.generate_column = task_config.get('generate_column', False)
        self.default_column = task_config['default_column']
        self.base_columns = task_config['base_column']
        self.table_metadata = task_config.get('table_metadata', [])
        self.column_info = task_config.get('column_info', [])

        self.base_table = task_config['base_table']
        self.extend_table = task_config['extend_table']
        self.extend_config_table = task_config['extend_config_table']

        self.dict_metadata = {}

        for dict_info in dict_tables:
            self.dict_metadata[dict_info['name']] = dict_info
        
    def get_connection(self):
        return self.dbtools.get_connection()
    
    def get_source_connection(self):
        return self.source_dbtools.create_engine()
    
    # 根据表明查询列信息，列名和注释
    def get_column_info(self, table_name):
        sql = f'''
        SELECT 
            att.attname AS column_name,
            col_description(att.attrelid, att.attnum) AS comment
        FROM 
            sys_class cls,
            sys_attribute att
        WHERE 
            cls.relname = '{table_name}'
            AND att.attrelid = cls.oid
            AND att.attnum > 0;
                '''
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    data = {}
                    for item in result :
                        data[item[0]] = item[1]
                    return data
                except Exception as e:
                    logging.error(f"查询列信息[{table_name}]失败", exc_info=True)
   
    # 从其他表更新当前表的缺失字段
    def update_column_info(self, update_column_info):
        if update_column_info is None:
            return
        columns = update_column_info['columns']
        right_table = update_column_info['right_table']

        right_table_name = right_table['name']
        current_on = right_table['current_on']
        right_on = right_table['right_on']

        sql_columns = []
        for column in columns:
            current_column = column['name']
            from_column = column['from']
            sql_columns.append(f' eq."{current_column}" = fac."{from_column}"')

        sql = f'''
        UPDATE
            {self.base_table} eq
        SET
            {','.join(sql_columns)}
        FROM
            {right_table_name} fac
        WHERE
            eq."{current_on}" = fac."{right_on}"
        '''
        with self.get_connection()as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    conn.commit()
                except Exception as e:
                    logging.error(f"更新补充字段数据失败", exc_info=True)
    
    def remove_prefix(self,text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text
    # 将源表的数据清晰为目标表的格式
    def convert_table(self, table_name):
        query = f"SELECT * FROM {table_name}"

        conn = self.get_source_connection()
        df = pd.read_sql_query(query, conn)

        base_column_names = list(map(lambda x: x['name'], self.base_columns))

        rname_config = {}
        ext_columns = []
        base_columns = []
        column_comment = {}
        for column in self.column_info[table_name]:
            name = column['name']
            comment = column['comment']
            dest = column.get('dest', None)
            column_comment[name] = comment
            if dest is not None:
                rname_config[name] = dest
                if dest in base_column_names:
                    base_columns.append(dest)
            else:
                ext_columns.append(name)


        df = df.rename(columns=rname_config)
        
        ext_rename_config = {'value': 'field_value', 'variable': 'field_code'}
        id_vars = [] 
        for base_column_item in self.base_columns:
            mode = base_column_item.get('mode', '')
            col_name = base_column_item['name']
            to_col_name = base_column_item.get('to', '')
            from_column_name = base_column_item.get('from', '')
            dict_table = base_column_item.get('dict','')
            value_prefix = base_column_item.get('value_prefix',None)
            type_name = self.table_metadata[table_name]
            has_column = from_column_name in df.columns
            if mode == 'type_code':
                df[col_name] = self.dict_name_map['epfaclas_term'][type_name]
                base_columns.append(col_name)
            elif mode == 'type_name':
                df[col_name] = type_name
                base_columns.append(col_name)
            elif mode == 'id2name':
                if has_column:
                    if value_prefix is not None:
                        df[col_name] = df[from_column_name].map(lambda x: self.remove_prefix(self.dict_id_map[dict_table].get(x, x), value_prefix))
                    else:
                        df[col_name] = df[from_column_name].map(self.dict_id_map[dict_table])
                    base_columns.append(col_name)
            elif mode == 'name2id':
                if has_column:
                    if value_prefix is not None:
                        df[col_name] = df[from_column_name].map(lambda x: self.add_prefix(self.dict_name_map[dict_table].get(x, x), value_prefix))
                    else:
                        df[col_name] = df[from_column_name].map(self.dict_name_map[dict_table])
                    base_columns.append(col_name)
            elif mode == 'copy':
                id_vars.append(col_name)
                ext_rename_config[col_name] = to_col_name
            else:
                pass

        # 处理默认字段
        for column_item in self.default_column:
            col_name = column_item['name']
            value = column_item['value']
            df[col_name] = value
            base_columns.append(col_name)
        
        # 截取基础字段
        base_datas = df[base_columns]

        # 截取扩展字段
        ext_columns = set(ext_columns) - set(base_columns) | set(id_vars)
        ext_datas = df[list(ext_columns)]

        self.logger.debug("基础字段：\n%s", base_datas)
        self.logger.debug("扩展字段：\n%s", ext_datas)
        
        # 使用 pd.melt() 将列转换为行
        value_vars = ext_datas.columns.difference(id_vars)
        melted_df = pd.melt(ext_datas, id_vars=id_vars, value_vars=value_vars)

        # 插入中文名称列
        melted_df['field_name'] = melted_df['variable'].map(column_comment)
        melted_df = melted_df.rename(columns=ext_rename_config)

        self.logger.debug("列转行后的结果：\n%s", melted_df)
        return base_datas, melted_df
    # 构建列信息，帮助收集各表的列信息
    def build_column_config(self):
        for table_name in self.table_metadata:
            self.logger.info('正在处理：%s', table_name)

            # 获取列信息
            if self.generate_column:
                column_infos = self.get_column_info(table_name)
                columns_to_write = []
                for column_name in column_infos:
                    columns_to_write.append({
                        "name": column_name,
                        "dest": column_name,
                        'comment': column_infos[column_name]
                    })
                
                self.tables_config_to_write[table_name] = columns_to_write
        data = {
            "tasks": {
                "task1": {
                    "column_info": self.tables_config_to_write
                 }
            }
        }
         # 获取当前时间并格式化
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # 备份目录
        temp_dir = 'temp'
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
        temp_file_path = os.path.join(temp_dir, f'column_info_{timestamp}.yaml')
        with open(temp_file_path, 'w', encoding='utf-8') as file:
            yaml.dump(data, file, sort_keys=False, allow_unicode=True)
        self.logger.info("收集列信息完毕，文件已写入到：%s", temp_file_path)

    # 构建扩展列配置表的信息
    def build_ext_column_config(self):

        field_codes = []
        field_names = []
        eqfac_type_id_list = []
        eqfac_type_name_list = []
        is_basic_dict_list = []
        show_type_list = []
        foreign_key_dic_list = []
        foreign_key_code_list = []
        foreign_key_name_list = []

        for table_name in self.column_info:
            table_comment = self.table_metadata[table_name]
            self.logger.info('正在处理：%s[%s]', table_name, table_comment)
            for column in self.column_info[table_name]:
                name = column['name']
                comment = column['comment']
                dest = column.get('dest', None)
                is_basic_dic = column.get('is_basic_dic', 0)
                show_type = column.get('show_type', 'Input')
                foreign_key_dic = column.get('dict', None)
                if dest is None:
                    field_codes.append(name)
                    field_names.append(comment)
                    epfaclas_term_data = self.dict_name_map['epfaclas_term']
                    eqfac_type_id = None
                    if table_comment in epfaclas_term_data:
                        eqfac_type_id = epfaclas_term_data[table_comment]
                    eqfac_type_id_list.append(eqfac_type_id)
                    eqfac_type_name_list.append(table_comment)
                    is_basic_dict_list.append(is_basic_dic)
                    show_type_list.append(show_type)
                    foreign_key_dic_list.append(foreign_key_dic)
                    metadata = {'id_field': '', 'name_field': ''}
                    if foreign_key_dic in self.dict_metadata:
                        metadata = self.dict_metadata[foreign_key_dic]
                    foreign_key_code_list.append(metadata['id_field'])
                    foreign_key_name_list.append(metadata['name_field'])

        df = pd.DataFrame({
            'field_code': field_codes,
            'field_name': field_names,
            'eqfac_type_id': eqfac_type_id_list,
            'eqfac_type_name': eqfac_type_name_list,
            'is_basic_dic': is_basic_dict_list,
            'show_type': show_type_list,
            'foreign_key_dic': foreign_key_dic_list,
            'foreign_key_code': foreign_key_code_list,
            'foreign_key_name': foreign_key_name_list,
            'del_flag':0
        })
        df['id'] = range(1, len(df) + 1)
        return df
    def run(self, task_id):
        self.logger.info('正在执行：%s', self.task_config.get('title', task_id))

        if self.generate_column:
            self.build_column_config()
            return
        
        all_base_datas = []
        all_ext_datas = []
        for table_name in self.column_info:
            self.logger.info('正在处理：%s', table_name)
            # 开始处理数据
            [base_datas, ext_datas] = self.convert_table(table_name)
           
            all_base_datas.append(base_datas)
            all_ext_datas.append(ext_datas)

        final_base_datas = concat(all_base_datas)
        final_base_datas['id'] = range(1, len(final_base_datas) + 1)
        # final_ext_datas = concat(all_ext_datas, ignore_index=True)
        final_ext_datas = concat(all_ext_datas)
        final_ext_datas['id'] = range(1, len(final_ext_datas) + 1)
        final_ext_datas['del_flag'] = 0

        ext_column_config = self.build_ext_column_config()

        self.logger.info("数据梳理完毕，等待写入数据库...")
        self.logger.debug("基础字段表：\n%s", final_base_datas)
        self.logger.debug("扩展字段表：\n%s", final_ext_datas)
        self.logger.debug("扩展字段配置表：\n%s", ext_column_config)


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

        ext_column_config.to_sql(name=f'{self.extend_config_table}', schema=schema_name, if_exists="append", con=engine, index=False)
        self.logger.info(f"写入[扩展字段配置表]: {self.extend_config_table} 数据完成")

        update_column_info = self.task_config.get('update_column', None)
        if update_column_info is not None:
            for column in update_column_info:
                self.update_column_info(column)
            self.logger.info("更新字段信息完成")
            
        self.logger.info('任务[%s]处理完成!', self.task_config.get('title', task_id))
        self.logger.info("\n---\n")

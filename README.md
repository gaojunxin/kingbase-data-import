# kingbase-data-import
自定义excel数据导入到kingbase中


## 安装依赖
```
poetry install
```

## 运行
```
poetry run python run.py
```

## 配置文件说明

```yml

# 数据库配置
database:
  host: localhost
  port: 54321
  user: system
  password: 123456
  dbname: demo
  schema_name: public


# 任务配置，下面可以有多个任务，会按顺序执行，判断active为true才会执行
tasks:
  # 任务1
  task-demo1:
    title: 任务1
    # 是否激活
    active: true
    # excel文件
    excel_file: /home/gaojunxin/数据初始化/demo1.xlsx
    # 设备基础信息表
    base_table: table1
    # 扩展信息表
    extend_table: table1_ext_data
    # 用到的所有字典表，最终会生成一个字典关系名称和字典id的dict对象，用于导入的时候根据名称获取外键的编号
    dict_tables:
      # 字典名称
      - name: epfaclas_term
      # id字段
        id_field: id
      # 名称字段
        name_field: name
    # 默认字段，值是固定的，或者需要程序中动态注入的，目前关键字：id, sheet_name 默认在处理阶段会解析为对应的变量
    default_column:
      - name: id
      - name: create_by
        value: 1
      - name: update_by
        value: 1
      - name: status
        value: '0'
      # 设备类型名称
      - name: eqled_term_typ_name
        value: "sheet_name"
    # 基础信息列
    base_column:
      # 字段0的数据库列名
      - name: column_0
      # 字段0的excel列索引
        index: 0
      # 字段1的数据库列名
      - name: column_1
      # 字段1的excel列索引
        index: 1
    # 外键编号列，会从上面定义的外键字典信息中根据名称关联到编号，并且给设置上值再插入
    fk_id_column:
      # 编号字段名称
      - name: eqled_term_bo_code
      # 对应的名称字段名称
        name_field: eqled_term_bo_name
      # 对应的字典表名称
        dict_table: com_organization
      # 有个特殊的需求，需要在根据名称获取编号的时候，需要拼接一个前缀
        value_prefix: "有限"
    # 扩展信息列
    ext_column:
      # 扩展字段列名
      - name: eqled_id
      # 扩展字段的值，这里写id会是自动注入基础字段中的id
        value: id

  # 任务2
  task-demo2:
    title: 任务2
    # 是否激活
    active: false
    # excel文件
    excel_file: /home/gaojunxin/数据初始化/demo2.xlsx
    base_table: table2
    extend_table: table2_ext_data
    default_column:
      - name: id
      - name: create_by
        value: 0
      - name: update_by
        value: 0
      - name: status
        value: "'0'"
    base_column:
      - name: facled_term_bo_name
    ext_column:
      - name: facled_id
      # 设备名称
      - name: facled_name
```
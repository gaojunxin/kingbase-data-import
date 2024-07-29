# kingbase-data-import
根据配置文件导入数据的脚本，目前支持两种模式
- excel导入模式
- 数据库导入模式

excel导入模式支持配置数据库字段和excel列之间的映射关系，支持从字典表中获取id和名称
数据库导入模式支持从多个数据表中配置和目标数据表的映射关系，然后进行多表合并

以上两个模式都支持将基础字段外的字段识别为扩展字段，并导入扩展字段表。


## 安装依赖
```
poetry install
```

## 运行
```
poetry run python main.py

# 打包
poetry run  pyinstaller -c --onefile .\main.py
```

## 配置文件说明

全局配置，config.yaml

```yml
# 数据库配置
database:
  db1:
    host: 192.168.0.2
    port: 54321
    user: system
    password: 123456
    dbname: demo
    schema_name: public
  db2:
    host: 192.168.0.3
    port: 54321
    user: system
    password: 123456
    dbname: demo
    schema_name: public

# 用到的所有字典表，最终会生成一个字典关系名称和字典id的dict对象，用于导入的时候根据名称获取外键的编号
dict_tables:
  # 字典名称
  - name: epfaclas_term
  # id字段
    id_field: id
  # 名称字段
    name_field: name
    # 所在数据库
    db: demo
```

任务配置在tasks目录下，程序会扫描所有tasks目录下的yaml文件并解析
```yml


# 任务配置，下面可以有多个任务，会按顺序执行，判断active为true才会执行
tasks:
  # 任务1
  task-demo1:
    title: 任务1
    # 是否激活
    active: true
    # 任务的执行顺序，多个任务的时候会用到
    order: 2
    # 任务执行的数据库，会和全局配置config.yaml中的配置结合使用
    db: demo
    # 任务类型，支持table和excel
    type: table
    # excel文件
    excel_file: /home/gaojunxin/数据初始化/demo1.xlsx
    # 设备基础信息表
    base_table: table1
    # 扩展信息表
    extend_table: table1_ext_data
    # 扩展字段配置
    extend_config_table: table1_ext_field_conf
    # 在直接导入的数据无法匹配到数据的时候，最后阶段可以配置下面的配置来执行更新数据
    update_column:
      # 更新排序字段
      - order_column:
        columns:
          - name: order_type
            from: sort
        # 连接的右表
        right_table:
          # 右表的名字
            name: epfaclas_term
            # 当前表与右表关联的字段
            current_on: eqled_type_code
            # 右表的关联字段
            right_on: id
    # 在插入数据前清空表
    clear_table:
      - name: table1
      - name: table1_ext_data
    # 默认字段，值是固定的，或者需要程序中动态注入的，目前关键字：id, sheet_name 默认在处理阶段会解析为对应的变量
    default_column:
      - name: id
      - name: create_by
        value: 1
      - name: update_by
        value: 1
      - name: status
        value: '0'
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
      # 分公司编号
      - name: xxx_term_bo_code
      # 根据哪一列来获取当前字段的值
        from: column_0
      # 从哪个字典表中根据名称查询编号
        dict: com_organization
      # 会自动给名称添加前缀再去字典中查询
        value_prefix: "有限"
      # 当前字段的查询模式，name2id表示根据名称查询id，还支持id2name模式，上面的字段相同
        mode: name2id
      - name: facled_type_name
      # 模式为type_name的时候会根据当前sheet页的名字或表的名字来获取他对应的类型名字
        mode: type_name
      # 设备类型编码
      - name: facled_type_code
      # 模式为type_code的时候会根据当前sheet页的名字或表的名字来获取他对应的类型编码
        mode: type_code
      - name: id
      # 模式为copy的时候会从基础信息表中把当前字段复制到扩展字段表中，并且根据to字段的名字重命名当前id字段名，做到基础字段表和扩展信息表之间的关联 
        mode: copy
        to: eqled_id

```
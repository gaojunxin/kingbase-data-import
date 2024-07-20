#!/usr/bin/python
# -*- coding: UTF-8 -*-
import datetime
import os

import logging
import yaml
from excel_runner import ExcelRunner

if __name__ == "__main__":
    with open('config.yaml', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    db_config = config['database']
    task_config = config['tasks']
    runner = ExcelRunner(db_config)
    for task_id in task_config:
        item_config = task_config[task_id] 
        if item_config['active'] == True:
            runner.run_task(task_id, item_config)
    
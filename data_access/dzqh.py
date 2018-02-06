#!/usr/bin/env python
# encoding: utf-8
"""
@Auther: simon
@Filename: test1.py
@Creation Time: 2018/02/05 16:31
@Version : Python 3.5.3
"""
import pymysql.cursors
# 连接MySQL数据库
connection = pymysql.connect(host='192.168.2.74', port=3306, user='Dongli', password='dzqh2018', db='sys',
                             charset='utf8mb4',  cursorclass=pymysql.cursors.DictCursor)

# 通过cursor创建游标
cursor = connection.cursor()

# 执行数据查询
sql = "SELECT * FROM `sys_config`"
cursor.execute(sql)

#查询数据库数据
result = cursor.fetchall()

for data in result:
    print(data)

connection.close()

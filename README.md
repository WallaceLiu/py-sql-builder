# 概述
我的场景主要基于pyspark，其已经提供了强大的数据处理能力，我们当然可以选择全部用pyspark来实现，但往往编写spark sql已经足够，而且可读性比纯代码要好，因此，实际中，注意实现方式的选择。

通常的ETL功能java编写的居多，但针对我的场景。
- 首先，虽然编写sql容易，但很难维护。整个业务下来sql将很长，核心业务被淹没；
- 其次，不需要这么强大的ETL功能，pyspark已经足够强大；
- 最后，也不想用java，不想增加自己负担，毕竟scala也是个不错的选择，编码效率很高。

因此，本文旨在用py通过注解实现一个ETL功能，定制sql语句。

# 工程结构
文件 | 说明
---|---
const.py | 常量，必要的符号和sql关键字
etl.py | etl，基本注解功能
etl.ipnb | 测试注解

# SQL注解
注解| 说明 | 形如
---|---|---
Select | 过滤 | select {field} from {table} {filter}
SubQuery | 子查询 | select field from (select * from table) alias
Aggregate | 聚合 | {subquery} {group}
AggregateSel | 过滤并聚合 | select {field} from {table} {filter} {group}
Join | 表连接 | select {field} from ({before}) {before_alias} \[right&#124;left&#124;inner\] join ({after}) {after_alias} on({on})
SelectMap | 过滤再映射 | select {field_map} from (select {field} from {table} where {filter}) {alias}
Map | 映射| select mapper from ({subquery}) {alias}
InsertOverWrite | 插入hive表 | INSERT OVERWRITE TABLE {table} {partition} {select}

> 注意：**{}** 符号，标识注解传入的参数
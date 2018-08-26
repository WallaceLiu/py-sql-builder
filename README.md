# 概述
我的场景——基于pyspark，其已经提供了强大的数据处理能力，我们当然可以选择全部用pyspark来实现，但往往编写spark sql已经足够，而且可读性比纯代码要好，因此，实际中，注意实现方式的选择。

通常的ETL功能java编写的居多，但针对我的场景。
- 首先，不需要这么强大的ETL功能，pyspark已经足够强大；
- 其次，也不想用java，毕竟scala是个不错的选择。不想增加自己负担。

因此，本文旨在用py通过注解实现一个etl功能，定制sql语句。

# 工程结构
文件 | 说明
---|---
const.py | 常量，必要的符号和sql关键字
etl.py | etl，基本注解功能
etl.ipnb | 测试注解

# SQL注解
注解| 说明
---|---
select | select {field} from {table} {filter}
subquery | select field from (select * from table) alias
Aggregate | {sub-query} {group}
AggregateSel | select {field} from {table} {filter} {group}
Join | select {field} from ({before}) {before_alias} \[right&#124;left&#124;inner\] join ({after}) {after_alias} on({on})
selectMap | select field_map from (select field from table where f) alias
Map | select mapper from (sub-query) alias
InsertOverWrite | INSERT OVERWRITE TABLE {table} {partition} {select}

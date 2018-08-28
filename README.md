# 概述
我的场景主要基于pyspark，其已经提供了强大的数据处理能力，我们当然可以选择全部用pyspark来实现，但往往编写spark sql已经足够，而且可读性比纯代码要好，因此，实际中，注意实现方式的选择。

通常的ETL功能java编写的居多，但针对我的场景。
- 首先，虽然编写sql容易，但很难维护。做完整个业务后，sql很冗长。核心业务淹没其中；
- 其次，不需要这么强大的ETL功能，pyspark已经足够强大；
- 最后，也不想用java，不想增加自己负担，毕竟scala也是个不错的选择，编码效率很高。

因此，本文旨在用py通过注解实现一个类似ETL功能，定制生成sql语句。

# 工程结构
文件 | 说明
---|---
const.py | 常量，必要的符号和sql关键字
sql_builder.py | etl，基本注解功能
test.ipynb | 测试注解

# SQL注解

注解| 说明 | 形如
---|---|---
Select | 过滤 | select {field} from {table} {filter}
SubQuery | 子查询 | select {field} from ({subquery}) {alias}
AggregateSel | 过滤再聚合 | select {field} from {table} {filter} {group}
Aggregate | 聚合 | {subquery} {group}
Join | 表连接 | select {field} from ({before}) {before_alias} \[right&#124;left&#124;inner\] join ({after}) {after_alias} on({on})
Map | 映射| select mapper from ({subquery}) {alias}
SelectMap | 过滤再映射 | select {field_map} from (select {field} from {table} where {filter}) {alias}
InsertOverWrite | 插入hive表 | INSERT OVERWRITE TABLE {table} {partition} {select}

> 注意：**{}** 符号，表示注解传入的参数

> - Select注解，用于过滤，不提供分组功能
> - SubQuery注解，用于封装子查询
> - AggregateSel注解，用于将分组和过滤
> - Aggregate注解，用于过滤后的分组
> - Join注解，用于表链接
> - Map注解，用于对字段应用的函数
> - SelectMap注解，用于先过滤，再对字段应用函数
> - InsertOverWrite注解，用于封装

假设有如下示例表。

- Persons 表

Id_P	| LastName	| FirstName	| Address	| City
---|---|---|---|---
1	| Adams	| John	| Oxford | Street London
-|-|-|-|- 

- Orders 表

Id_O	| OrderNo	| Id_P
---|---|---
1	| 77895	| 3
-|-|-


## Select和SubQuery注解注解
```python
@Select(field=['Id_P', 'LastName', 'FirstName'], f=["1=1"])
def persons():
    return 'Persons'
```
结果：
```sql
select Id_P,LastName,FirstName from Persons WHERE 1=1
```
## AggregateSel注解
```python
@AggregateSel(field=['Id_P','count(*)'], f=['1=1'], group=['Id_P'])
def orders():
    return 'Orders'
```
结果：
```sql
select Id_P,count(*) from Orders WHERE 1=1 GROUP BY Id_P
```

## Join注解
```python
@Select(field=['*'], f=[])
def Persons():
    return 'Persons'

@Select(field=['*'], f=[])
def Orders():
    return 'Orders'

sql_p = Persons()
sql_o = Orders()
print('filter person:{}'.format(sql_p))
print('filter order: {}'.format(sql_o))

@Join(t="eq", before=sql_o, after=sql_p, 
      field=['t1.OrderNo','concat(t2.FirstName, t2.LastName) as name'], 
      on=['t1.Id_P=t2.Id_P'],before_alias="t1", after_alias="t2")
def join(before_field=[], after_field=[]):
    return {
        "before_field": before_field,
        "after_field": after_field
    }

s3 = join()
```
结果：
```sql
select t1.OrderNo,concat(t2.FirstName, t2.LastName) as name from (select * from Orders ) t1
    join (select * from Persons ) t2 on(t1.Id_P=t2.Id_P)
```
## SelectMap和Map注解
```python
@SelectMap(
    field=['id', 'start_date', 'len', "regexp_replace(regexp_replace(y, '\\\\]', ''), '\\\\[', '') as y"],
    field_map=['id', 'start_date', 'len', 'split(y) as y'], alias="t1",
    f=["type='ttt'", "and", "key='sales'"])
def select_map(table):
    return {"table": table}

s1 = select_map(table="mytable")
print('s1:  {}'.format(s1))

@Map(mapper=['id', 'start_date', 'len', "concat(',', d1, d2)"], alias="t2")
@Map(mapper=['id', 'start_date', 'len', 'd1', 'd2'], alias="t2")
def mapper():
    sql = select_map("mytable")
    return sql

s2 = mapper()
```
结果：
```sql
select id,start_date,len,concat(',', d1, d2) from (select id,start_date,len,d1,d2 from (select id,start_date,len,split(y) as y from (select id,start_date,len,regexp_replace(regexp_replace(y, '\\]', ''), '\\[', '') as y from mytable WHERE type='ttt' and key='sales') t1) t2) t2
```

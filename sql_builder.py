# -*- coding: utf-8 -*-
__author__ = 'liuning11@jd.com'

from functools import wraps
from const import BLANK_SPACE, BLANK, COMMA, FULL_STOP, SQL_WHERE, SQL_GROUP_BY, SQL_PARTITION

class SqlBuilderBase(object):
    __WHERE = SQL_WHERE + BLANK_SPACE
    __GROUP = SQL_GROUP_BY + BLANK_SPACE
    __PARTITION = SQL_PARTITION

    def __init__(self):
        pass

    @staticmethod
    def check_list(v):
        """

        :param v:
        :return:
        """
        if isinstance(v, list):
            return False if v is None or len(v) <= 0 else True
        else:
            raise TypeError('Type Error, v is list.')

    @staticmethod
    def check_alias(v):
        """

        :param v:
        :return:
        """
        if isinstance(v, str):
            return False if v is None or len(v) <= 0 else True
        else:
            raise TypeError('Type Error, v is string.')

    @staticmethod
    def concat_list_by_sep(v, sep):
        if isinstance(v, list) and isinstance(sep, str):
            return sep.join(v)
        else:
            return v

    @staticmethod
    def group_by(v, sep=COMMA):
        """

        :param v:
        :param sep:
        :return:
        """
        return BLANK if not SqlBuilderBase.check_list(v) \
            else SqlBuilderBase.__GROUP + SqlBuilderBase.concat_list_by_sep(v, sep)

    @staticmethod
    def filter(v):
        """

        :param v:
        :return:
        """
        return BLANK if not SqlBuilderBase.check_list(v) \
            else SqlBuilderBase.__WHERE + SqlBuilderBase.concat_list_by_sep(v, BLANK_SPACE)

    @staticmethod
    def field(v, alias=None):
        """

        :param v:
        :param alias:
        :return:
        """
        if SqlBuilderBase.check_list(v):
            if alias is None or len(alias) <= 0:
                return SqlBuilderBase.concat_list_by_sep(v, COMMA)
            else:
                field = [alias + FULL_STOP + f for f in v]
                return SqlBuilderBase.concat_list_by_sep(field, COMMA)
        else:
            return v

    @staticmethod
    def on(v):
        """

        :param v:
        :return:
        """
        if SqlBuilderBase.check_list(v):
            return SqlBuilderBase.concat_list_by_sep(v, BLANK_SPACE)
        else:
            return v

    @staticmethod
    def partition(partition):
        """

        :param partition:
        :return:
        """
        if SqlBuilderBase.check_list(partition):
            return "{}({})".format(SqlBuilderBase.__PARTITION, SqlBuilderBase.concat_list_by_sep(partition, COMMA))
        else:
            return BLANK


class Select(SqlBuilderBase):
    """
    select field from table where f
    """
    _template = """select {field} from {table} {filter}"""

    def __init__(self, field, f=[]):
        """

        :param field:   list    字段和映射字段
        :param f:       list    过滤条件
        """
        self._field = field
        self._filter = f

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            return self._template.format(field=Select.field(self._field),
                                         table=fn(*args, **kwargs),
                                         filter=Select.filter(self._filter))

        return wrapped


class SubQuery(SqlBuilderBase):
    """
    子查询语句

    select field from (select * from table) alias
    """
    _template = "select {field} from ({subquery}) {alias}"

    def __init__(self, field, alias):
        """

        :param field:   list    字段和字段映射
        :param alias:   string  别名
        """
        self._field = field
        self._alias = alias

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            return self._template.format(field=SubQuery.field(self._field),
                                         subquery=fn(*args, **kwargs),
                                         alias=self._alias)

        return wrapped


class Aggregate(SqlBuilderBase):
    """
    聚合子查询
    """
    _template = "{subquery} {group}"

    def __init__(self, field, field_aggr, aggrs, group, alias):
        self._field = field
        self._field_aggr = field_aggr
        self._alias = alias
        self._aggrs = aggrs
        self._group = group

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            aggr_field = self._field_aggr
            for aggr in self._aggrs:
                aggr_field = ["{}({})".format(aggr, f) for f in aggr_field]

            @SubQuery(field=self._field + aggr_field, alias=self._alias)
            def _sql():
                result = fn(*args, **kwargs)
                return result['table']

            sql = _sql()
            return self._template.format(subquery=sql, group=Aggregate.group_by(self._group))

        return wrapped


class AggregateSel(SqlBuilderBase):
    """
    直接聚合
    """
    _template = """select {field} from {table} {filter} {group}"""

    def __init__(self, field, f, group):
        self._field = field
        self._filter = f
        self._group = group

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):            
            return self._template.format(field=AggregateSel.field(self._field),
                                         table=fn(*args, **kwargs),
                                         filter=AggregateSel.filter(self._filter),
                                         group=AggregateSel.group_by(self._group))

        return wrapped


class Join(SqlBuilderBase):
    """
    连接，包括等值、左连接、右连接、内连接
    """
    _template = {
        "eq": """
    select {field} from ({before}) {before_alias}
    join ({after}) {after_alias} on({on})
    """,
        "right": """
    select {field} from ({before}) {before_alias}
    right join ({after}) {after_alias} on({on})
    """,
        "left": """
    select {field} from ({before}) {before_alias}
    left join ({after}) {after_alias} on({on})
    """,
        "inner": """
    select {field} from ({before}) {before_alias}
    inner join ({after}) {after_alias} on({on})
    """
    }

    def __init__(self, t, before, after, field, on, before_alias="", after_alias=""):
        self._type = t.lower() if isinstance(t, str) and t is not None else "eq"
        self._before = before
        self._before_alias = before_alias if Join.check_alias(before_alias) else "t1"
        self._after = after
        self._after_alias = after_alias if Join.check_alias(after_alias) else "t2"
        self._field = field

        self._on = on

    def _get_field(self, before_field, after_field):
        """
        获得字段列表
        :return:
        """
        if Join.check_list(before_field) and Join.check_list(after_field):
            return [
                Join.field(before_field, self._before_alias),
                Join.field(after_field, self._after_alias)
            ]
        else:
            return self._field

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            result = fn(*args, **kwargs)
            before_field = [] if "before_field" not in result else result['before_field']
            after_field = [] if "after_field" not in result else result['after_field']
            field = Join.concat_list_by_sep(self._get_field(before_field, after_field), COMMA)

            return self._template[self._type].format(field=field,
                                                     before=self._before,
                                                     before_alias=self._before_alias,
                                                     after=self._after,
                                                     after_alias=self._after_alias,
                                                     on=Join.on(self._on))

        return wrapped


class InsertOverWrite(SqlBuilderBase):
    """
    INSERT OVERWRITE hive表
    """
    _template = """
        INSERT OVERWRITE TABLE {table} {partition}
        {select}
   """

    def __init__(self, select, partition=None):
        """

        :param select:      string select语句
        :param partition:   list    分区
        """
        self._select = select
        self._partition = partition

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            return self._template.format(table=fn(*args, **kwargs),
                                         partition=InsertOverWrite.partition(self._partition),
                                         select=self._select)

        return wrapped


class SelectMap(SqlBuilderBase):
    """
    select and map 语句
    形如：
    select field_map from (select field from table where f) alias
    """

    def __init__(self, field, field_map, alias, f=[]):
        """
        :param field:       list    字段
        :param field_map:   list    字段和映射字段
        :param alias:       string  别名
        :param f:           list    过滤条件
        """
        self._field = field
        self._field_map = field_map
        self._alias = alias
        self._filter = f

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            @SubQuery(field=self._field_map, alias=self._alias)
            @Select(field=self._field, f=self._filter)
            def _sql():
                result = fn(*args, **kwargs)
                return result['table']

            return _sql()

        return wrapped


class Map(SqlBuilderBase):
    """
    Map 映射函数
    """

    def __init__(self, mapper, alias):
        """

        select mapper from (sub query) alias

        :param mapper:  list    字段和映射字段
        :param alias:   string  别名
        """
        self._mapper = mapper
        self._alias = alias

    def __call__(self, fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            @SubQuery(field=self._mapper, alias=self._alias)
            def _sql():
                result = fn(*args, **kwargs)
                return result

            return _sql()

        return wrapped

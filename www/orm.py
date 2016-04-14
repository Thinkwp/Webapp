#!/bin/env python
#coding:utf-8
#Author:itxx00@gmail.com
import asyncio, logging

import aiomysql

def log(sql, args = ()):
    logging.info('SQL: %s' % sql)

#创建连接池
#我们需要创建一个全局的连接池，每个HTTP请求都可以从连接池中直接获取数据库连接。使用连接池的好处是不必频繁地打开和关闭数据库连接，而是能复用就尽量复用。

#连接池由全局变量__pool存储，缺省情况下将编码设置为utf8，自动提交事务
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    #调用一个子协程来创建全局连接池，create_pool的返回值是一个pool实例对象
    __pool = await aiomysql.create_pool(
            #设置连接的属性
            host = kw.get('post', 'localhost'),
            port = kw.get('port', 3306),
            user = kw['user'],
            password = kw['password'],
            db = kw['db'],
            charset = kw.get('charset', 'utf8'),
            autoconnect = kw.get('autoconnect', True),
            maxsize = kw.get('maxsize', 10),
            minsize = kw.get('minsize', 1),
            loop = loop
            )

#Select
#要执行SELECT语句，我们用select函数执行，需要传入SQL语句和SQL参数
#SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换。注意要始终坚持使用带参数的SQL，而不是自己拼接SQL字符串，这样可以防止SQL注入攻击。
#注意到yield from将调用一个子协程（也就是在一个协程中调用另一个协程）并直接获得子协程的返回结果。
#如果传入size参数，就通过fetchmany()获取最多指定数量的记录，否则，通过fetchall()获取所有记录。
async def select(sql, args, size = None):
    log(sql, args)
    global __pool
    #从连接池获取一条数据库连接
    async with __pool.get() as conn:
        #打开一个DictCursor，它与普通游标的不同在于以dict形式返回结果
        async with conn.cuesor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs

#Insert, Update, Delete
#要执行INSERT、UPDATE、DELETE语句，可以定义一个通用的execute()函数，因为这3种SQL的执行都需要相同的参数，以及返回一个整数表示影响的行数：
#execute()函数和select()函数所不同的是，cursor对象不返回结果集，而是通过rowcount返回结果数。
async def execute(sql, args, autocommit = True):
    log(sql)
    #打开一个普通游标
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cu:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
        except BaseExpection as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected
#构造占位符
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

#定义Field以及Field各种子类
#父域
class Field(object):
    #域的初始化，包括属性名、类型、是否是主键
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    #打印信息，依次是类名、属性类型、属性域
    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
#字符串域
class StringField(Field):
    #ddl用于定义数据类型
    #varchar，可变长度的字符串，100表示最长长度
    def __init__(self, name = None, primary_key = False, default = None, ddl = 'varchar(100)'):
        super().__init__(name, ddl, primary_key, default)
#布尔域
class BooleanField(Field):
    def __init__(self, name = None, default = False):
        super().__init__(name, 'boolean', False, default)
#整数域
class IntegerField(Field):
    def __init__(self, name = None, primary_key = False, default = 0):
        super().__init__(name, 'bigint', primary_key, default)
#浮点数域
class FloatField(Field):
    def __init__(self, name = None, primary_key = False, default = 0.0):
        super().__init__(name, 'real', primary_key, default)
#文本域
class TextField(Field):
    def __init__(self, name = None, default = None):
        super().__init__(name, 'text', False, default)

#将具体的子类如User的映射信息读取出来
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        #cls：当前准备创建的类对象，相当于self
        #name：类名，比如User继承自Model，当使用该元类创建User类时，name = User
        #bases：父类的元组
        #attrs：属性的字典
        if name == 'Model':#排除Model自身，因为Model类主要就是用来被继承的，其不存在与数据库的映射
            return type.__new__(cls, name, bases, attrs)

        #以下时针对“Model”的子类的处理，将被用于子类的创建，metaclass将隐式的被继承

        #获取表名，若没有定义__table__属性，将类名作为表名
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        #获取所有的Field和主键名
        mappings = dict()#用字典来储存属性与数据库表的列的映射关系
        fields = []#用于保存除主键以外的属性
        primarykey = None#用于保存主键
        #遍历类的属性，找出定义的域内的值，建立映射关系
        #k是属性名， v是其定义域
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v#建立映射关系
                if v.primary_key:
                    #找到主键
                    if primarykey:#若主键以存在，又找到一个主键，将报错
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primarykey = k
                else:
                    fields.append(k)#将非主键的属性加入field列表中
        if not primarykey:#没有找到主键也报错，每张表有且仅有一个主键
            raise StandardError('Primary key not found.')
        #从类属性中删除已加入映射字典的键，避免重名
        for k in mappings.keys():
            attrs.pop(k)
        #将非主键的属性变形，放入escaped_fields中，方便增删改查语句的书写
        escaped_fields = list(map(lambda f: '`%s`' %f, fields))
        attrs['__mappings__'] = mappings #保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primary_key #主键属性名
        attrs['__fields__'] = fields #除主键外的属性名
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primarykey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

#定义的是所有ORM映射的基类Model
class Model(dict, metaclass = ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setsttr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def findAll(cls, where = None, args = None, **kw):
        ' find object by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('oeder By')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]
    @classmethod
    async def findNumber(cls, selectField, where = None, args = None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        ' find object by primary Key. '
        rs = await select('%s where `%s` = ?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('faild to update by primary key: affected row: %s' %  rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)

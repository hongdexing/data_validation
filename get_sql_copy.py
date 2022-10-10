#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser


# 读取config.ini
def readconfig():
    read_config = ConfigParser.ConfigParser()
    read_config.read('config.ini')
    return read_config


# 获取批量sql
class Sql:
    def __init__(self, conf):
        self.conf = conf
        self.a_filter = conf.get('tables', 'a_filter')
        self.b_filter = conf.get('tables', 'b_filter')
        self.table1 = conf.get('tables', 'table1').replace(' ', '')
        self.table2 = conf.get('tables', 'table2').replace(' ', '')
        self.relation = conf.get('tables', 'relation').replace(' ', '')
        self.field1 = conf.get('field', 'table1').replace(' ', '')
        self.field2 = conf.get('field', 'table2').replace(' ', '')

    def get_base_sql(self):
        table1 = self.field1.split(',')
        table2 = self.field2.split(',')
        list1, list2, list3, list4, list5 = [], [], [], [], []

        for i in range(len(table1)):
            list1.append('a.' + table1[i] + ' as a_{}'.format(table1[i]))
            list2.append('b.' + table2[i] + ' as b_{}'.format(table2[i]))
            list3.append("if(case when a.{} is null and b.{} is null then false else nvl(a.{} != b.{}, True) end,'a_{}',null)".format(table1[i], table2[i], table1[i], table2[i], table1[i]))
            list4.append("case when a.{} is null and b.{} is null then false else nvl(a.{} != b.{}, True) end".format(table1[i], table2[i], table1[i], table2[i]))
            # case when a.{} is null and b.{} is null then false else nvl(a.{} != b.{}, True) end
            list5.append('a_{},b_{}'.format(table1[i], table2[i]))
        return [list1, list2, list3, list4, list5]

    def get_sql1(self):
        base_sql = self.get_base_sql()
        s = [base_sql[0][i] + ',' + base_sql[1][i] for i in range(len(base_sql[0]))]
        sql1 = 'select ' + "{}, concat_ws(',',{}) as diff from {} as a inner join {} as b on {} where 1=1 and ({})". \
            format(','.join(s), ','.join(base_sql[2]), self.table1, self.table2, self.relation,
                   ' or '.join(base_sql[3]))
        if len(self.a_filter) > 0:
            sql1 = sql1 + ' and ' + self.a_filter
        if len(self.b_filter) > 0:
            sql1 = sql1 + ' and ' + self.b_filter
        return sql1

    def get_sql2(self):
        base_sql = self.get_base_sql()
        s = self.get_sql1()
        sql2 = 'select ' + "{},single_diff from ({}) t lateral VIEW explode(split(diff, ',')) tmp_diff as single_diff". \
            format(','.join(base_sql[4]), s)
        return sql2

    def get_sql3(self):
        base_sql = self.get_base_sql()
        s = self.get_sql2()
        sql3 = 'select ' + "{},single_diff,row_number() over (partition by single_diff) rowNum from ({}) t".format(
            ','.join(base_sql[4]), s)
        return sql3

    def get_sql4(self):
        base_sql = self.get_base_sql()
        s = self.get_sql3()
        sql4 = 'select ' + "{},single_diff,rowNum from ({}) t where rowNum <= 50 limit 5000;".format(
            ','.join(base_sql[4]), s)
        return sql4

    def get_difference_count(self):
        sql5 = 'select t.single_diff,count(*) as numbers from ' + "({}) as t group by t.single_diff;".format(
            self.get_sql2())
        return sql5

    def get_abExist(self):
        base_sql = self.get_base_sql()
        a_filter = self.a_filter
        b_filter = self.b_filter
        if len(self.a_filter) == 0:
            a_filter = '1=1'
        if len(self.b_filter) == 0:
            b_filter = '1=1'

        aExist = 'select ' + "{} from {} as a where not exists (select 1 from {} as b where {} and {}) and {};".format(
            ','.join(base_sql[0]), self.table1, self.table2, self.relation, b_filter, a_filter)
        bExist = 'select ' + "{} from {} as b where not exists (select 1 from {} as a where {} and {}) and {};".format(
            ','.join(base_sql[1]), self.table2, self.table1, self.relation, a_filter, b_filter)
        return [aExist, bExist]


class Writer:
    def __init__(self, file_name, path='./sql', content=''):
        self.path = path
        self.file_name = 'check_' + file_name + '.sql'
        self.content = content

    def write_field(self):
        with open("{}/{}".format(self.path, self.file_name), 'w') as f:
            f.write(self.content)


if __name__ == '__main__':
    read_config = readconfig()
    sql = Sql(read_config)
    cross_sql = sql.get_sql4()
    difference_sql = sql.get_difference_count()
    aexist_sql = sql.get_abExist()[0]
    bexist_sql = sql.get_abExist()[1]
    content = '-- 表单A和表单B的交集差异明细' + '\n' + cross_sql + '\n\n\n' + \
              '-- 表单A和表单B的交集差异数量统计' + '\n' + difference_sql + '\n\n\n' + \
              '-- 表单A存在表单B不存在明细' + '\n' + aexist_sql + '\n\n\n' + \
              '-- 表单B存在表单A不存在明细' + '\n' + bexist_sql + '\n\n'
    path = ''
    file_name = read_config.get('default', 'name')
    w = Writer(file_name, content=content)
    w.write_field()

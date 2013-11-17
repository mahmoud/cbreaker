# -*- coding: utf-8 -*-

import sqlite3
from argparse import ArgumentParser

from dump_loader import MySQLDump

CL_TABLE_NAME = 'categorylinks'
CL_FIELD_NAMES = ('cl_from cl_to cl_sortkey cl_timestamp cl_sortkey_prefix'
                  ' cl_collation cl_type').split()

_LINK_TARGET_VAL = 'subcat'


def create_table(schema, location=':memory:'):
    conn = sqlite3.connect(location)
    conn.execute(schema)
    conn.commit()
    return conn


def main(filename):
    dump = MySQLDump(filename, CL_TABLE_NAME, CL_FIELD_NAMES)
    schema = dump.get_create_statement()
    conn = create_table(schema, 'cat_table.db')
    ins_query = (u'INSERT INTO %s VALUES (%s)'
                 % (CL_TABLE_NAME, ', '.join('?' * len(CL_FIELD_NAMES))))

    should_parse = lambda val_str: _LINK_TARGET_VAL in val_str
    should_keep = lambda val_tuple: val_tuple[-1] == _LINK_TARGET_VAL
    reader = dump.get_reader(should_parse=should_parse,
                             should_keep=should_keep)
    entries = reader.load(1000)
    while entries:
        cursor = conn.cursor()
        for a in entries:
            cursor.execute(ins_query, a)
        conn.commit()
        entries = reader.load(1000)
    count_query = 'SELECT COUNT(*) FROM %s' % CL_TABLE_NAME
    written_count = conn.execute(count_query).fetchone()[0]
    print 'loaded and wrote %s entries.' % written_count
    return


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()
    main(args.filename)

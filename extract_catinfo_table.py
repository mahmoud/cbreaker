# -*- coding: utf-8 -*-

import sqlite3
from argparse import ArgumentParser

from dump_loader import MySQLDump

CAT_TABLE_NAME = 'category'
CAT_FIELD_NAMES = 'cat_id,cat_title,cat_pages,cat_subcats,cat_files'.split(',')


def create_table(schema, location=':memory:'):
    conn = sqlite3.connect(location)
    conn.execute(schema)
    conn.commit()
    return conn


def main(filename):
    dump = MySQLDump(filename, CAT_TABLE_NAME, CAT_FIELD_NAMES)
    schema = dump.get_create_statement()
    conn = create_table(schema, 'cat_table.db')
    ins_query = (u'INSERT INTO %s VALUES (%s)'
                 % (CAT_TABLE_NAME, ', '.join('?' * len(CAT_FIELD_NAMES))))

    reader = dump.get_reader()
    entries = reader.load(1000)
    while entries:
        cursor = conn.cursor()
        for a in entries:
            cursor.execute(ins_query, a)
        conn.commit()
        entries = reader.load(1000)
    written_count = conn.execute('SELECT COUNT(*) FROM category').fetchone()[0]
    print 'loaded and wrote %s entries.' % written_count
    return


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    args = prs.parse_args()
    main(args.filename)

# -*- coding: utf-8 -*-

import os
import re
import ast
import time
import gzip
import codecs

from argparse import ArgumentParser

READ_SIZE = 2 ** 18  # 256kb
_LITERAL = r"('([^'\\]*(?:\\.[^'\\]*)*)'|\d+)"
_TUPLE_RE = re.compile(r"\(%s(,%s)*\)" % (_LITERAL, _LITERAL))


# TODO: table name and field autoguess/load


def main(filename, tablename):
    msd = MySQLDump(filename, tablename, [])
    reader = msd.get_reader()
    entries = reader.load(100)
    import pdb;pdb.set_trace()


class MySQLDump(object):
    def __init__(self, source_file, table_name, table_fields,
                 read_size=READ_SIZE, is_gzip=None, encoding='utf-8'):
        self.source_file = source_file
        self.table_name = table_name
        self.table_fields = list(table_fields)
        if is_gzip is None:
            is_gzip = source_file.endswith('.gz')
        self.is_gzip = is_gzip
        self.encoding = encoding
        self.read_size = read_size

        self.total_size = bytes2human(os.path.getsize(source_file), 2)

    def get_reader(self, *a, **kw):
        return TableReader(self, *a, **kw)

    def get_create_statement(self):
        return ('CREATE TABLE %s (%s);'
                % (self.table_name, ', '.join(self.table_fields)))

    def get_header(self):
        tmp_reader = self.get_reader(verbose=False)
        return tmp_reader._load_header()


class TableReader(object):
    def __init__(self, mysql_dump, **kwargs):
        self._msd = msd = mysql_dump
        self.start_time = time.time()
        self.cur_stmt_count = 0

        self.verbose = kwargs.pop('verbose', True)
        self.should_parse = kwargs.pop('should_parse', lambda entry_str: True)
        self.should_keep = kwargs.pop('should_keep', lambda entry_tuple: True)

        self._buff = u''
        self.decoder = codecs.getreader(msd.encoding)

        if msd.is_gzip:
            self._file_handle_enc = gzip.open(msd.source_file)
            self._get_cur_pos = lambda: self._file_handle_enc.fileobj.tell()
        else:
            self._file_handle_enc = open(msd.seource_file)
            self._get_cur_pos = lambda: self._file_handle_enc.tell()
        self._file_handle = self.decoder(self._file_handle_enc,
                                         errors='replace')

        self._insert_into_token = 'INSERT INTO `%s`' % msd.table_name
        self._header = ''

    def _load_header(self):
        data = self._file_handle.read(4096)
        self._header, self._buff = data.split(self._insert_into_token)
        if not self._header:
            raise ValueError('complete CREATE TABLE preamble not found'
                             ' (or exceeds 4kb); check that the table name '
                             ' %r is correct' % self._msd.table_name)
        return self._header

    def load(self, min_count=None):
        if not self._header:
            self._load_header()
        data = self._buff
        ret = []
        while data:
            data = self._file_handle.read(self._msd.read_size)
            self._buff += data

            ii_end = self._buff.find(self._insert_into_token, 11)
            if ii_end < 0:
                continue
            self.cur_stmt_count += 1
            stmt, self._buff = self._buff[:ii_end].strip(), self._buff[ii_end:]
            for m in _TUPLE_RE.finditer(stmt):
                group = m.group()
                if not self.should_parse(group):
                    continue
                val_tuple = ast.literal_eval(group)
                if not self.should_keep(val_tuple):
                    continue
                ret.append([v.decode('utf-8') if type(v) is str else v
                            for v in val_tuple])

            if self.verbose and self.cur_stmt_count % 5 == 0:
                cur_count = len(ret)
                cur_duration = round(time.time() - self.start_time, 2)
                cur_bytes = bytes2human(self._get_cur_pos(), 2)
                print cur_count, 'records.', cur_bytes,
                print 'out of', self._msd.total_size, 'read. (',
                print self.cur_stmt_count, 'statements)',
                print cur_duration, 'seconds.'

            if min_count and len(ret) >= min_count:
                return ret

        return ret


_SIZE_SYMBOLS = ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
_SIZE_BOUNDS = [(1024 ** i, sym) for i, sym in enumerate(_SIZE_SYMBOLS)]
_SIZE_RANGES = zip(_SIZE_BOUNDS, _SIZE_BOUNDS[1:])


def bytes2human(nbytes, ndigits=0):
    """
    >>> bytes2human(128991)
    '126K'
    >>> bytes2human(100001221)
    '95M'
    >>> bytes2human(0, 2)
    '0.00B'
    """
    abs_bytes = abs(nbytes)
    for (size, symbol), (next_size, next_symbol) in _SIZE_RANGES:
        if abs_bytes <= next_size:
            break
    hnbytes = float(nbytes) / size
    return '{hnbytes:.{ndigits}f}{symbol}'.format(hnbytes=hnbytes,
                                                  ndigits=ndigits,
                                                  symbol=symbol)


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('filename')
    prs.add_argument('tablename')
    args = prs.parse_args()
    main(args.filename, args.tablename)

# -*- coding: utf-8 -*-

import json
from argparse import ArgumentParser

from dump_loader import MySQLDump

CAT_TABLE_NAME = 'category'
CAT_FIELD_NAMES = 'cat_id,cat_title,cat_pages,cat_subcats,cat_files'.split(',')

CL_TABLE_NAME = 'categorylinks'
CL_FIELD_NAMES = ('cl_from cl_to cl_sortkey cl_timestamp cl_sortkey_prefix'
                  ' cl_collation cl_type').split()


def load_page_id_map(page_filename):
    page_dump = MySQLDump(page_filename)
    should_parse = lambda val_str: '14,' in val_str
    should_keep = lambda val_tuple: val_tuple[1] == 14
    cat_page_reader = page_dump.get_reader(should_parse=should_parse,
                                           should_keep=should_keep)
    cat_id_map = {}
    entries = cat_page_reader.load(1000)
    while entries:
        for e in entries:
            cat_id_map[e[0]] = e[2]
        entries = cat_page_reader.load(1000)
    return cat_id_map


def main(page_filename, catlink_filename):
    name_id_map = json.load(open('name_id_map.json'))
    id_name_map = {v: k for k, v in name_id_map.iteritems()}
    #id_name_map = load_page_id_map(page_filename)
    # name_id_map = {v: k for k, v in page_id_map.iteritems()}
    # json.dump(name_id_map, open('name_id_map.json', 'w'))
    cl_dump = MySQLDump(catlink_filename)
    should_parse = lambda val_str: 'subcat' in val_str
    should_keep = lambda val_tuple: val_tuple[-1] == 'subcat'
    cl_reader = cl_dump.get_reader(should_parse=should_parse,
                                   should_keep=should_keep)
    cl_entries = cl_reader.load()
    cat_links = []
    missing_cat_links = []
    for cl in cl_entries:
        try:
            # relation is "contains", i.e., "Sports" contains "Wrestling"
            cat_links.append([cl[1], id_name_map[cl[0]]])
        except KeyError:
            missing_cat_links.append(cl)
    with open('cat_links.json', 'w') as f:
        json.dump(cat_links, f)
    with open('missing_cat_links.json', 'w') as f:
        json.dump(missing_cat_links, f, indent=2)
    import pdb;pdb.set_trace()
    return


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('page_dump')
    prs.add_argument('catlinks_dump')
    args = prs.parse_args()
    main(args.page_dump, args.catlinks_dump)

# -*- coding: utf-8 -*-

import json
from argparse import ArgumentParser

import networkx as nx
from networkx import simple_cycles


def main(link_json_filename):
    cat_links = json.load(open(link_json_filename))
    graph = nx.DiGraph()
    for i, link in enumerate(cat_links):
        graph.add_edge(*link)
        if i % 100000 == 0:
            print i, 'links loaded'
    cyc_iter = simple_cycles(graph)
    import pdb;pdb.set_trace()


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('link_json_filename')
    args = prs.parse_args()
    main(args.link_json_filename)

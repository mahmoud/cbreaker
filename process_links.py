# -*- coding: utf-8 -*-

import json
from argparse import ArgumentParser

import networkx as nx
from networkx import simple_cycles


def simple_cycles2(G):
    comps = nx.strongly_connected_component_subgraphs(G)
    print 'found %s strongly-connected component subgraphs' % len(comps)
    all_cycles = []
    do_pdb = True
    for c in comps:
        if len(c) == 1:
            continue
        else:
            for cycle in nx.simple_cycles(c):
                print 'found cycle, length: %s' % len(cycle)
                all_cycles.append(cycle)
                if len(all_cycles) % 10 == 0 and do_pdb:
                    import pdb;pdb.set_trace()
    return all_cycles


def main(link_json_filename):
    cat_links = json.load(open(link_json_filename))
    graph = nx.DiGraph()
    for i, link in enumerate(cat_links):
        graph.add_edge(*link)
        if i % 100000 == 0:
            print i, 'links loaded'
    del cat_links
    cycles = simple_cycles2(graph)
    import pdb;pdb.set_trace()


if __name__ == '__main__':
    prs = ArgumentParser()
    prs.add_argument('link_json_filename')
    args = prs.parse_args()
    main(args.link_json_filename)

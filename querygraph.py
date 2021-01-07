#!/usr/bin/env python3

import json
import sys

def fmt(s, l):
    import itertools

    result = ""
    llen = 0
    def pred(word):
        nonlocal llen
        if llen > l:
            llen = 0
            return False
        else:
            llen += (len(word) + 1)
            return True
    
    ls = s.split()
    while len(ls) > 0:
        currentline = list(itertools.takewhile(pred, ls))
        result += " ".join(currentline)
        ls = ls[len(currentline):]
        result += "\\l"
    
    return result

if __name__ == "__main__":
    graph = json.load(sys.stdin)
    plan_nodes = set( [ (d["plan_node"], d["simpleString"]) for d in graph ] )
    query_nodes = set( [ ("ex" + str(d["execution_id"]), d["description"] ) for d in graph if d["parent"] == -1] )
    plan_edges = set( [ (d["parent"], d["plan_node"]) for d in graph if d["parent"] != -1 ])
    query_edges = set( [ ("ex" + str(d["execution_id"]), d["plan_node"]) for d in graph if d["parent"] == -1 ])

    print("digraph {")

    for node in list(plan_nodes):
        no_gpu=""
        if "Gpu" not in node[1]:
            no_gpu = 'fontcolor="orange", color="orange", '
        if "GpuRowToColumnar" in node[1] or "GpuColumnarToRow" in node[1]:
            no_gpu = 'color="red", fontcolor="red", '
        print('"%s" [label="%s", %sshape="box", labeljust="l", fontname="Fira Code"];' % (node[0], fmt(node[1], 45), no_gpu))
    
    for node in list(query_nodes):
        print('"%s" [label="%s", fontcolor="blue", shape="box", style="rounded", labeljust="l", color="blue", fontname="Fira Code"];' % (node[0], fmt(node[1], 45)))
    
    for edge in list(plan_edges.union(query_edges)):
        print('"%s" -> "%s"' % (edge[0], edge[1]))

    print("}")
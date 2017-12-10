import graphviz as gv

class Graph():
    def __init__(self):
        self.graphs = []
        self.graph = ""

    def get_id(self, cluster_ix, node, port=None):
        id_t = '{}_{}'.format(cluster_ix, node)
        if port is not None:
            id_t = '{}:p{}'.format(id_t, port)
        return id_t

    def node_label(self, records):
        header = '<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADING="4">'
        footer = '</TABLE>>'
        node_name = records[0]['node_name']
        if node_name[0] == '_':
            rows = ['<TR><TD BGCOLOR="azure2"><I>{}</I></TD></TR>'.format(records[0]['node_name'])]
        else:
            rows = ['<TR><TD><U>{}</U></TD></TR>'.format(records[0]['node_name'])]
        for r in sorted(records,key = lambda x: x['date']):
            rows.append('<TR><TD ALIGN="LEFT" PORT="p{}">{}_ {}_ {}</TD></TR>'.format(
                r['port'],
                r['date'],
                '${:,.0f}'.format(r['amount']),
                r['narrative']
            ))
        return('{}{}{}'.format(header,'\n'.join(rows),footer))

    def add_nodes_with_meta(graph, cluster_ix, nodes):
        for node_name,recs in nodes.items():
            graph.node(get_id(cluster_ix, node_name), label=node_label(recs), shape='plaintext')
        return graph

    def add_marker_nodes(graph, idx, prefix, len=4):
        for i in range(len-1):
            graph.node_attr.update(shape='none',width = '0', height = '0', label='')
            #graph.edge_attr.update(dir='none')
            graph.edge(get_id(idx,'{}{}'.format(prefix, i)),
                       get_id(idx,'{}{}'.format(prefix, i+1)),dir='none', style='invis')

#first subgraph serves as marker line
d=digraph('',node_attr={'shape':'none','width':'0','height':'0','label':''},
         edge_attr = {'dir':'none'})
for i in range(3):
    d.edge(get_id(len(graphs),'p{}'.format(i)),
           get_id(len(graphs),'p{}'.format(i+1))
           )
graphs.append(d)
#

dt_gr.groupby('context')
for (name,group) in dt_gr.groupby('context'):
    idx = len(graphs)
    d = digraph('cluster_'+str(idx),
               graph_attr={'label':'{}[{}]'.format(name, idx)}
               )
    d.body.extend(['rankdir=LR'])
    add_marker_nodes(d, idx, 'i', 5) 
    #add records. node's list is compiled alongside in order which
    #allows to shift some transaction's metadata into corresponding
    #nodes 'labels'. This is our attempt to display as much information
    #as possible withouth excessive cluttering of chart
    recs = [] #list of tuples
    nodes = {} #dict of list of dicts
    port = 0
    for r in group.to_dict(orient='records'):
        recs.append((
                ( get_id(idx,r['source'],port),
                  get_id(idx,r['dest'], port),
                ), {'label':''}
            ))
        sign = 1
        for n in ['dest','source']:
            node_name = r[n]
            nodes.setdefault(node_name,[]).append({
                'node_name':node_name,
                'port':port,
                'date':r['date'],
                'amount':r['usd'] * sign,
                'narrative':r['narrative']
            })
            sign *= -1
        port+=1
    d = add_nodes_with_meta(d, idx, nodes)
    d = add_edges(d, recs)
    graphs.append(d)

def rank_objects(graph, objects):
    '''objects is a dict with key=index of an cluster, value = list of objects'''
    objects_fq = []
    for ix, nodes in objects.items():
        if type(nodes) == str:
            objects_fq.append("\"" + get_id(ix, nodes) + "\"")
        else:
            objects_fq +=["\"" + get_id(ix, node) + "\"" for node in nodes]

    statement = '{{rank=same; {} }}'.format(' '.join(objects_fq))
    graph.body.extend([statement])
    return statement

#apply manual fine tuning of layout
print(rank_objects(graphs[2],{2:'company a, investor z'.split(',')}))


#
d_maj = digraph(engine='dot')
d_maj.body.extend(['rankdir=LR'])
for d in graphs:
    d_maj.subgraph(d)

d_maj.render('tmp/investments')
d_maj

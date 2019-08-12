import ipywidgets as widgets
from IPython.display import display, HTML
import pandas as pd, numpy as np
import fnmatch
import yaml, json
import re, datetime

#import ipywidgets as widgets, qgrid, ipysheet
#import re
'''
Graphic representation for an arbitrary dataset wich aims to show units flow
through the group of nodes.
NOTES:
    - input data fields (must be in this order, original columns name are discarded):
      - date      : date of a transaction          : datetime
      - domain    : node from which units departed : str
      - corr_node : node to which units flow       : str
      - units     : aggregated values              : integer, float
Args:
    - threshold: combine less significant corr_node into one single 'other' node. Threshold
      can be specified in units (int) or as a number of branches in output graph (n[N], where N - 
      is total number of branches)
'''
class Test():
    def __init__(self, dt_records):
        self.dt = dt_records
        self.a = 12

class UnitsFlow():
    controls_dump_file = 'units_flow_controls.tmp'
    def __init__(self, dt_records):
        self.a = 2
        self.dt = dt_records

    def get_interface(self):

        #w_output = widgets.Output(layout={'width':'100%', 'height':'520px'})
        w_output = widgets.Output()

        def tmp_click(b):
            b.description = 'Done!'
            t_options = list(self.w_domain.options)
            t_options.append('z')
            self.w_domain.options = t_options
            with w_output:
                print('done_done')

        def w_save_click(b):
            """dump values of selected interactive controls to json file """
            CONTROLS = 'w_date_start,w_date_end,w_domain,w_self_tr'.split(',')
            def json_handler(obj):
                if isinstance(obj, (datetime.datetime, datetime.date)):
                    return obj.isoformat()

            config_out = {}
            for k in CONTROLS:
                config_out[k] = getattr(self,k).value

            with open(self.controls_dump_file, 'w') as fl:
                json.dump(config_out, fl, default=json_handler)
            #b.description = 'Saved'


        def w_restore_click(b):
            """restore values previously saved by w_save_click"""
            import sys
            def json_decoder(d):
                d_out = {}
                for k,v in d.items():
                    try:
                        v = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S')
                    except:
                        pass
                    d_out[k] = v
                return d_out

            try:
                with open(self.controls_dump_file, 'r') as fl:
                    config_in = json.load(fl, object_hook=json_decoder)
            except FileNotFoundError:
                return

            for k,v in config_in.items():
                widget = getattr(self, k)
                if widget._view_name == 'SelectMultipleView' and type(v) == list:
                    v = tuple(set(widget.options) & set(v))
                widget.value = v

            #b.description = 'Restored'

        @w_output.capture(clear_output=True)
        def w_run_click(b):
            date_start, date_end = [w.value for w in (self.w_date_start, self.w_date_end)]
            domains_selected = self.w_domain.value
            query_str = '(@date_start <= date <= @date_end) and (domain in @domains_selected)'
            dt_after_filter = self.dt.query(query_str)
            display(HTML(self.get_graph(dt_after_filter).render()))

        # ---
        def get_years_range():
            """ Returns:
               list of years
            """
            t_year_begin, t_year_end = [d.year for d in self.dt['date'].apply(['min','max'])]
            return list(range(t_year_begin, t_year_end + 1))

        # ===

        # ---
        self.w_date_start = widgets.DatePicker(
                value = self.dt['date'].min(),
                description = "begin"
                )

        # ---
        self.w_date_end = widgets.DatePicker(
                value = self.dt['date'].max(),
                description = "end"
                )

        # ---
        t_domain = sorted(self.dt['domain'].unique())
        self.w_domain = widgets.SelectMultiple(
                options = t_domain,
                value = [t_domain[-1]],
                rows = 5,
                description = 'from',
                disabled = False
                )
        del t_domain
        # ---
        self.w_save = widgets.Button(description = "Save")
        self.w_save.on_click(w_save_click)
        # ---
        self.w_restore = widgets.Button(description = "Restore")
        self.w_restore.on_click(w_restore_click)
        # ---
        self.w_run = widgets.Button(description = "Run")
        self.w_run.on_click(w_run_click)
        # ---
        self.w_self_tr = widgets.Checkbox(
                value=True,
                description='Ignore transactions with self',
                )
        # ---
        self.w_net_tr = widgets.Checkbox(
                value=False,
                description='net off opposite transactions'
                )
        # ---
        self.w_align_domains = widgets.Checkbox(
                value=False,
                description='align domain nodes'
                )
        # ---
        w_html = widgets.HTML()


        v_box_buttons = widgets.VBox([self.w_save, self.w_restore, self.w_run])
        v_box_dates = widgets.VBox([self.w_date_start, self.w_date_end])
        v_box_options = widgets.VBox([self.w_self_tr, self.w_net_tr, self.w_align_domains])
        h_box_toolbar = widgets.Box([v_box_buttons, v_box_dates, self.w_domain, v_box_options])

        return widgets.VBox([h_box_toolbar, w_output])

    def drill(self, pat):
        """ returns transactions for a single edge.
        """
        columns = []
        matched_labels_txt = []
        matched_labels = []
        for lbl, records in self.gr_dt:
            lbl_txt = '_'.join(lbl[:])
            if len(columns) == 0:
                columns = records.columns
            if fnmatch.fnmatch(lbl_txt, pat):
                matched_labels_txt.append(lbl_txt)
                matched_labels.append(lbl)

        if len(matched_labels_txt) > 1:
            lbl_txt = '\n'.join(matched_labels_txt)
            print("More then one edge satisfies search criteria:\n>>>>>>>\n" + lbl_txt + "\n>>>>>>>>")
            #return None
            #return "More then one edge satisfies search criteria:\n>>>>>>>\n" + lbl_txt + "\n>>>>>>>>"
        if len(matched_labels) > 0:
            return pd.concat([self.gr_dt.get_group(l) for l in matched_labels],axis=0).ex.totals()
        else:
            return pd.DataFrame(columns = columns)


    @classmethod
    def map_value(cls, item, map_scheme):
        """ returns Series for groupping purpose (designed to be fed to
            function pd.DataFrame.groupby
            map_scheme is yaml file of format
        """
        for map_l in map_scheme:
            got_match = True
            map_out = map_l['_map_']
            for k,v in map_l.items():
               if k[0] == k[-1] == '_':
                   continue
               elif not v.match(item):
                   got_match = False
                   break
            if got_match:
                break

        return map_out

    @classmethod
    def map_values(cls, s, map_scheme):
        for map_l in map_scheme:
            map_out = map_l['_map_']
            for k,v in map_l.items():
                if k[0] == k[-1] == '_':
                    continue
                elif not v.match(s['k']):
                    return None

        return map_out


    @classmethod
    def compile_mapping(cl, map_scheme_raw):
        """ compile regular expression for all key-value pair,
        except where the key is a special name, which starts and ends with '_'
        """
        import copy

        map_scheme = copy.deepcopy(map_scheme_raw)
        for k_t, lower_group in map_scheme.items():
            for item in lower_group:
                for k,v in item.items():
                    if k[0] == k[-1] == '_':
                        continue
                    item[k] = re.compile(v, re.IGNORECASE)

        return map_scheme



    def get_graph(self, dt=[], threshold=None):
        """ Compile diagram from list of transactions, collected from various
        sources (domain).  Each transaction must contain at least the following
        fields(columns):
        - date       : date of a transaction          : datetime
        - domain     : node from which units departed : str
        - corr_node  : node to which units flow       : str
        - units      : aggregated values              : integer, float
        Attr         :
        - self.dt :                                : pd.DataFrame
        """
        from graphviz import Digraph

        #rel_master dict: {sorted([node_a, node_b]): node_a|node_b}
        rel_master = {}
        nodes = {}
        if len(dt) == 0:
            dot = Digraph(comment = "node movement report",
                      filename='units_flow_chart.gv',
                      format='svg',
                      graph_attr={'rankdir':'LR'})
            return dot
        domains = sorted(dt['domain'].unique())

        def check_relationship_master(domain, corr_node):
            """input records are collected from different domains, which may have
            transactions between each other. Such transactions are usualy reflected
            by both sides, receiving and sending. One set shall be removed.  The
            following methods are implemented:
            - detect first transaction between two domains and elect 'master'
              domain in this 'relationship'. Records for this relationship
              recorded under other domain are ignored
            - use order of master list of domains to determine 'winning' domain
            """
            key = ','.join(sorted([domain, corr_node]))
            rel_m = rel_master.get(key)
            if not rel_m:
                rel_master[key] = domain
                return True
            elif rel_m == domain:
                return True
            else:
                return False


        def create_node(id, rep=None, shape='ellipse'):
            if nodes.get(id):
                return

            if not rep:
                rep = id
            if id in domains:
                shape = 'component'
            dot.node(id, rep, shape=shape)
            nodes[id] = 'ok'

        def create_edge(node_master, node_corr, units_total):
            pref = ''
            if units_total > 0:
                pref = '*'
                node_from, node_to  = node_corr, node_master
            else:
                units_total = -units_total
                node_from, node_to  = node_master, node_corr

            total_str = "{2}${0:,.{1}f}".format(units_total, 0, pref)
            dot.edge(node_from, node_to, label=total_str)

        def link_nodes(lbl, total):
            """Args:
                lbl: ('domain','corr_node'): tuple
               NOTE:
               Records can be duplicated, as single transaction can be
               (and in some systems, must be) reflected by both sides,
               sender and receiver
            """
            if self.w_self_tr.value and (lbl[0] == lbl[1]):
                return
            if not check_relationship_master(domain=lbl[0], corr_node=lbl[1]):
                return

            for lb in lbl:
                create_node(lb)

            create_edge(*lbl, total)

        dot = Digraph(comment = "node movement report",
                      filename='units_flow_chart.gv',
                      format='svg',
                      graph_attr={'rankdir':'LR'})

        self.gr_dt = dt.groupby(['domain',
                    'corr_node',
                    dt['units'].apply(lambda x: 'dt' if x>0 else 'ct')
        ])

        totals = self.gr_dt['units'].sum()
        if self.w_net_tr.value:
            labels = totals.index.droplevel(2).unique()
        else:
            labels = totals.index

        for lbl in labels:
            link_nodes(lbl[:2], totals.loc[lbl].sum())

        if self.w_align_domains.value:
            with dot.subgraph(name='scale', node_attr={'shape':'box'}) as c:
                dot.edges(list(range(len(domains))))
            for d in enumerate(domains):
                #with dot.subgraph() as s:
                   #s.attr(rank='same')
                   #s.node()







        return dot

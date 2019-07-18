import ipywidgets as widgets
import pandas as pd, numpy as np
import re
'''
Graphic representation for an arbitrary dataset wich aims to show units flow through the group of nodes
NOTES:
    - input data fields (must be in this order, original columns name are discarded):
      - date        : date of a transaction          : datetime
      - node_origin : node from which units departed : str
      - node_target : node to which units flow       : str
      - units       : aggregated values              : integer, float
      - narrative   : for drill-down / debugging     : str
ARGS:
    - threshold: combine less significant node_target into one single 'other' node. Threshold
      can be specified in units (int) or as a number of branches in output graph (n[N], where N - 
      is total number of branches)
'''
def 
def get_flow_widget_set(dt_records, threshold=0):

    t_years = sorted(dt_records['date'].dt.year.unique())
    w_years = widgets.SelectMultiple(
            options = t_years,
            value = [t_years[-1]],
            rows = 4,
            description = 'years',
            disabled = False
            )

    t_node_origin = sorted(dt_records['node_origin'].unique())
    w_node_origin = widgets.SelectMultiple(
            options = t_node_origin,
            value = [t_node_origin[-1]],
            rows = 5,
            description = 'source_origin',
            disabled = False
            )

    out = widgets.interactive_output(get_flow_graph, {'years':w_years, 'node_origin':w_node_origin})
    w_box = widgets.VBox([widgets.HBox([w_years, w_node_origin]), out])
    return w_bok

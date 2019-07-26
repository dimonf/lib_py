import ipywidgets as widgets
from IPython.display import display
import pandas as pd, numpy as np
#import ipywidgets as widgets, qgrid, ipysheet
#import re
'''
Graphic representation for an arbitrary dataset wich aims to show units flow through the group of nodes
NOTES:
    - input data fields (must be in this order, original columns name are discarded):
      - date        : date of a transaction          : datetime
      - node_from : node from which units departed : str
      - node_to : node to which units flow       : str
      - units       : aggregated values              : integer, float
Args:
    - threshold: combine less significant node_to into one single 'other' node. Threshold
      can be specified in units (int) or as a number of branches in output graph (n[N], where N - 
      is total number of branches)
'''
def get_interface(dt_records):

    def w_save_click(b):
        b.description = 'Done!'
        t_options = list(w_node_from.options)
        t_options.append('z')
        w_node_from.options = t_options
        with w_output:
            print('done_done')

    def w_restore_click(b):
        with w_output:
            print(w_date_start.value, type(w_date_start.value))

    def w_run_click(b):
        pass

    # ---
    def get_years_range():
        """ Returns:
           list of years
        """
        t_year_begin, t_year_end = [d.year for d in dt_records['date'].apply(['min','max'])]
        return list(range(t_year_begin, t_year_end + 1))

    # ===
    w_date_start = widgets.DatePicker(
            value = dt_records['date'].min(),
            description = "begin"
            )

    # ---
    w_date_end = widgets.DatePicker(
            value = dt_records['date'].max(),
            description = "end"
            )

    # ---
    t_node_from = sorted(dt_records['node_from'].unique())
    w_node_from = widgets.SelectMultiple(
            options = t_node_from,
            value = [t_node_from[-1]],
            rows = 5,
            description = 'from',
            disabled = False
            )
    del t_node_from
    # ---
    w_save = widgets.Button(description = "Save")
    w_save.on_click(w_save_click)
    # ---
    w_restore = widgets.Button(description = "Restore")
    w_restore.on_click(w_restore_click)
    # ---
    w_run = widgets.Button(description = "Run")
    # ---
    w_output = widgets.Output()
    # ---


    v_box_buttons = widgets.VBox([w_save, w_restore, w_run])
    v_box_dates = widgets.VBox([w_date_start, w_date_end])
    h_box_toolbar = widgets.Box([v_box_buttons, v_box_dates, w_node_from])

    return widgets.VBox([h_box_toolbar, w_output])

def get_node_names(dt_row, node_from_col, node_to_col, map_nodes):
    """ designed to be called by pd.DataFrame.apply method
        Args:
          dt_row: a row from a dataframe
          node_from_col: collumn name
          node_to_col: column name
        Returns:
          tuple for node_from / node_to columns, after

    """

def get_report(dt_records, threshold=0):
    pass

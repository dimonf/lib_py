import ipywidgets as widgets
import pandas as pd

'''
  interactive dt-ct table

  dtct.columns: ['name','period','cat','usd','cc','narr']
  usage:
  import dtct_utils
  from IPython.display import display
  dtct = pd.read_csv(jup_utils.get_closest_file('_data/dtct-master.csv'),dtype={'period':str})
  display(dtct_utils.get_dtct_widget_set(dtct))
'''


def get_dtct_widget_set(dtct):
    w_list = widgets.Checkbox(
        value = False,
        description = "Full list",
        disabled=False,
    )

    t_years = sorted(dtct['period'].unique())
    w_years = widgets.SelectMultiple(
            options = t_years,
            value = [t_years[-1]],
            rows = 4,
            description = 'years',
            disabled = False,
    )
    del t_years

    w_source = widgets.Dropdown(
        options=list(dtct['cc'].unique()),
        description = "Source:",
        disabled = False
    )

    w_dtct = widgets.Dropdown(
        options=['by_value','by_category'],
        description = 'DtCt:',
        disabled=False
    )

    w_interco = widgets.Dropdown(
        options = ['altogether','hide','separate'],
        description='InterCo',
        disabled=False
    )

    w_regex = widgets.Text(
        value = '',
        placeholder = 'regexp',
        disabled=False
    )
    #
    def get_dtct_table(ls_output, years, source, dtct_method, interco, interco_reg):
        '''ls_output: True/False  #whether list of records instead of summary is shown
           years: [2015, 2017]
           source: "maersk"
           dtct_method: ["by_value","by_category"]
           interco: ["altogether","hide", "separate"]
           interco_reg: r"(?!something)" '''

        period_begin, period_end = min(years), max(years)
        #
        dt_out = dtct.query('@period_begin <= period <= @period_end').loc[dtct['cc'] == source]
        if interco == 'hide':
            dt_out = dt_out[~dt_out['name'].str.contains(interco_reg, case=False)]
        mapping = dt_out['cat'] if dtct_method == 'by_category' else (dt_out['usd']>0).map({True:'dt',False:'ct'})
        if interco == 'separate':
            mapping = mapping.mask(dt_out['name'].str.contains(interco_reg, case=False), dt_out['name'])
        if ls_output:
            with pd.option_context('display.max_rows',None):
                if interco == 'separate':
                    interco_bool = dt_out['name'].str.contains(interco_reg, case=False)
                    print(pd.concat([dt_out[interco_bool].ex.rtotal('usd'),
                                     pd.DataFrame(columns=dt_out.columns, index = [max(dt_out.index)+1],
                                                  data = [['-' for i in list(dt_out.columns)]]),
                                     dt_out[~interco_bool].ex.rtotal('usd')], sort=False))
                    #print(dt_out[interco_bool], '\n',dt_out[~interco_bool])
                else:
                    print(dt_out)
        else:
            with pd.option_context('display.max_rows', None):
                print(dt_out.groupby([dt_out['period'], mapping]).sum().unstack(0).ex.totals())

    out = widgets.interactive_output(get_dtct_table,{'ls_output':w_list,'years':w_years,'source':w_source,
                                     'dtct_method':w_dtct, 'interco':w_interco, 'interco_reg': w_regex})
    w_box = widgets.VBox([widgets.HBox([w_list, w_years, w_source, w_dtct, w_interco, w_regex]), out])
    #w_box = widgets.HBox([out, widgets.VBox([w_list, w_years, w_source, w_dtct, w_interco, w_regex])])

    return w_box

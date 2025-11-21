from collections import namedtuple, defaultdict

from pandas.core.computation.parsing import clean_column_name
from scipy.stats import gmean
import pandas as pd
import numpy as np

import s2soptions
from equivalence import duplicates
#import plotter



NODATA = '~'



BenchmarkResult = namedtuple('BenchmarkResult', [
    'query',
    'name',
    'option',
    'time',
    'error',
    'speedup'
])


def get_equivalence(query, option_str):
    """
    Returns a list with the all the options (compacted string)
    that compile the given query as the given option string
    """
    try:
        return next(eq for eq in duplicates[query] if option_str in eq)
    except StopIteration:
        raise ValueError(f'Cannot find the equivalence class of the option {option_str} for query {query}')


def read_all_as_df(fname, df_filter=None):
    """
    Read all results from a CSV file: results/all.csv.
    Returns a pandas dataframe from the CSV data

    Note. the input file is expected to be generated with
    the jmhutil.py script
    """

    df = pd.read_csv(fname)
    return df if df_filter is None else df_filter(df)



def to_df(query_results):
    return pd.DataFrame([row._asdict() for res in query_results.values() for row in res])



def get_perf_row(df, query, option_str):
    """
    Return a single row extracted from the given dataframe given a query and an option name
    """
    perf_line = df[(df['query'] == query) & (df['option_str'] == option_str)]
    if len(perf_line) == 0:
        print('MISSING DATA', query, option_str)
        return defaultdict(lambda: None)
    return perf_line.iloc[0]


def get_perf(df, query, option_str):
    """
    Return a pair (time, error) extracted from the given dataframe given a query and an option name
    """
    row = get_perf_row(df, query, option_str)
    return row['time'], row['err']



def df_to_latex(df, names):
    # Pivot the dataframe to have queries as rows and names as columns
    pivot_time = df.pivot(index='query', columns='name', values='time')
    pivot_error = df.pivot(index='query', columns='name', values='error')
    pivot_speedup = df.pivot(index='query', columns='name', values='speedup')

    # Order columns based on the provided names
    def clean_col(column, default=0):
        column = column.map(lambda x: np.nan if str(x).lower() in ('nan', 'none') else x)
        return column.fillna(default, inplace=False)

    pivot_time = clean_col(pivot_time[names])
    pivot_error = clean_col(pivot_error[names])
    pivot_speedup = clean_col(pivot_speedup[names], default=1)

    # Start LaTeX table
    latex_str = '\\begin{tabular}{' + 'rr|' * (len(names) - 1) + 'rr}\n'

    latex_str += '\\toprule\n'
    latex_str += ' & \\textbf{Baseline}'
    if len(names) > 2:
        latex_str += ' & ' +' & '.join([f'\\multicolumn{{2}}{{c|}}{{\\textbf{{{name}}}}}' for name in names[1:-1]])
    latex_str += f' & \\multicolumn{{2}}{{c}}{{\\textbf{{{names[-1]}}}}} \\\\ \n'

    latex_str += '\\midrule\n'

    # Fill table rows
    for query in pivot_time.index:
        latex_str += '\\textbf{' + query + '} & '

        name = names[0]
        time = pivot_time.at[query, name]
        error = pivot_error.at[query, name]
        latex_str += f'{int(time)}$\\pm${int(error)} & '

        row_values = []
        for name in names[1:]:
            time = pivot_time.at[query, name]
            error = pivot_error.at[query, name]
            speedup = pivot_speedup.at[query, name]
            if error == NODATA:
                right_border = '' if name == names[-1] else '|'
                # row_values.append(f' \\multicolumn{{2}}{{c{right_border}}}{{{time}}}')
                row_values.append(f'{time} & \\textbf{{{speedup:.2f}x}}')
            else:
                row_values.append(f'{int(time)}$\\pm${int(error)} & \\textbf{{{speedup:.2f}x}}')

        latex_str += ' & '.join(row_values) + ' \\\\ \n'

    # Eval geomean
    latex_str += ' \hline \n \\multicolumn{2}{l|}{\\textbf{Geo Mean}} &'

    row_values = []
    for name in names[1:]:
        tmp_df = df[df['name'] == name]
        speedup_column = pd.to_numeric(tmp_df['speedup'], errors='coerce').fillna(1.0)
        row_values.append(f'& \\textbf{{{gmean(speedup_column):.2f}x}}')
    latex_str += ' & '.join(row_values) + ' \\\\ \n'


    # End LaTeX table
    latex_str += '\\bottomrule\n'
    latex_str += '\\end{tabular}'

    return latex_str



def vertical_pivot_table(df):
    names = df['name'].unique()

    # Pivot the table to have queries as columns
    df_pivot = df.pivot(index='query', columns='name', 
                              values=["name", "time", "error", "speedup", "option", "query"])

    # 2 lines (with speedup version)
    def transform_cell(name, time, error, speedup):
        if name == 'B':
            return f'\makecell{{{time:.0f}+-{error:.0f}}}' 
        if speedup == NODATA:
            return ''
        return f'\makecell{{{time:.0f}+-{error:.0f} ({speedup:.2f}x)}}' 
    
    def transform_row(row):
        return [transform_cell(row['name'][q], row['time'][q], row['error'][q], row['speedup'][q]) for q in names]
        
    df_transformed = df_pivot.apply(transform_row, axis=1, result_type='expand')

    # Rename columns to remove multi-index
    df_transformed.columns = names
    return df_transformed



def to_file(filename, content):
    with open(filename, 'w') as f: 
        f.write(content)


def df_to_table_file(df, fname_prefix):
    to_file(fname_prefix+'.md', df.to_markdown())
    to_file(fname_prefix+'.tex', df.to_latex().replace('+-', '$\pm$'))
    
    
def table_options(df, fname):
    base = s2soptions.stream_baseline
    base_opt_str = base.compact_str()
    base_variant_name = base.variant_name()
    o1 = base.with_option('fuseFilters', 'fuseFilters')

    o2 = base.with_option('join_converter', 'MAPMULTI')
    o1o2 = o1.with_option('join_converter', 'MAPMULTI')
    
    row_options = [base, o1, o2, o1o2]
    row_options_names = ['B', 'O1', 'O2', 'O1+O2']
    row_options_strrepr = [o.compact_str() for o in row_options]
    non_base_options_list = list(zip(row_options, row_options_names, row_options_strrepr))[1:]

    queries = df['query'].unique()
    results = {}
    for query in queries:
        results[query] = query_data = []
        baseline_equivalence = get_equivalence(query, base_variant_name)
        btime, berr = get_perf(df, query, base_opt_str)
        query_data.append(BenchmarkResult(query, 'B', base_opt_str,  btime, berr, 1))
        missing_o1, missing_o2 = False, False
        speedup_o1, speedup_o2 = 1., 1.
        for opt, name, strrepr in non_base_options_list:
            if opt.variant_name() in baseline_equivalence:
                if name == 'O1':
                    missing_o1 = True
                elif name == 'O2':
                    missing_o2 = True
                otime, oerr, speedup = '=B', NODATA, 1.0
            else:
                otime, oerr = get_perf(df, query, strrepr)
                speedup = btime/otime
                if name == 'O1': speedup_o1 = speedup
                if name == 'O2': speedup_o2 = speedup
                if name == 'O1+O2':
                    if missing_o1 and missing_o2:
                        otime, oerr, speedup = '=B', NODATA, 1.
                    elif missing_o1:
                        otime, oerr, speedup = '=O2', NODATA, speedup_o2
                    elif missing_o2:
                        otime, oerr, speedup = '=O1', NODATA, speedup_o1
            query_data.append(BenchmarkResult(query, name, strrepr,  otime, oerr, speedup))

    table_df = to_df(results)
    to_file(fname, df_to_latex(table_df, row_options_names))


def table_vs_imperative(df, fname):
    stream = s2soptions.stream_best_sequential
    stream_opt_str = stream.compact_str()
    stream_name = 'Stream'

    imperative = s2soptions.ImperativeOption()
    imperative_opt_str = imperative.compact_str()
    imperative_name = 'Imperative'
    row_names = [stream_name, imperative_name]

    queries = df['query'].unique()
    results = {}
    for query in queries:
        results[query] = query_data = []
        stream_time, stream_err = get_perf(df, query, stream_opt_str)
        imperative_time, imperative_err = get_perf(df, query, imperative_opt_str)
        if stream_time is not None and imperative_time is not None:
            speedup = stream_time / imperative_time
            query_data.append(BenchmarkResult(query, stream_name, stream_opt_str, stream_time, stream_err, 1))
            query_data.append(BenchmarkResult(query, imperative_name, imperative_opt_str, imperative_time, imperative_err, speedup))

    table_df = to_df(results)
    table_df['name'] = pd.Categorical(table_df['name'], categories=row_names, ordered=True)
    to_file(fname, df_to_latex(table_df, row_names))



def table_parallel(df, fname):
    base = s2soptions.stream_best_sequential
    base_opt_str = base.compact_str()
    base_variant_name = base.variant_name()

    pu = base.with_option('multi_threading', 'PU')
    cg = base.with_option('multi_threading', 'CG')
    cgcc = base.with_option('multi_threading', 'CGCC')
    
    row_options = [base, pu, cg, cgcc]
    row_options_names = ['B', 'PU', 'CG', 'CGCC']
    non_base_options_list = list(zip(row_options, row_options_names))[1:]

    queries = df['query'].unique()
    results = {}
    for query in queries:
        results[query] = query_data = []
        baseline_equivalence = get_equivalence(query, base_variant_name)
        btime, berr = get_perf(df, query, base_opt_str)
        query_data.append(BenchmarkResult(query, 'B', base_opt_str,  btime, berr, 1))
        checkweird = {'B': 1}
        for opt, name in non_base_options_list:
            strrepr = opt.compact_str()
            otime, oerr = get_perf(df, query, strrepr)
            speedup = btime/otime if opt.variant_name() not in baseline_equivalence else NODATA
            query_data.append(BenchmarkResult(query, name, strrepr,  otime, oerr, speedup))
            checkweird[name] = speedup

    table_df = to_df(results)
    table_df['name'] = pd.Categorical(table_df['name'], categories=row_options_names, ordered=True)
    to_file(fname, df_to_latex(table_df, row_options_names))

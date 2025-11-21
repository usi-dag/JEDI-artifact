from collections import namedtuple
import numpy as np
import pandas as pd

import s2soptions


# vars for data header
# join_converter = 'join_converter'
# multi_threading = 'multi_threading'
# fuse_filters = 'fuse_filters'
#
# flatmap = 'FLATMAP'
# map_multi = 'MAPMULTI'
# join_type_values = flatmap, map_multi
#
#
# data_header_options = (
#     join_converter,
#     multi_threading,
#     fuse_filters
# )
# data_header = ('query', 'time', 'err', 'platform', 'option_str', *data_header_options)

# Utilities for CSV conversion
data_header = ('query', 'time', 'err', 'option_str')
CSVItem = namedtuple('Item', data_header)

def json_to_df(json_file, convert_func, **convert_func_params):
    import json
    with open(json_file) as f:
        data = json.load(f)
        return pd.DataFrame([convert_func(row, **convert_func_params) for row in data])


def tpch_to_item(row):
    time, err = row['primaryMetric']['score'], row['primaryMetric']['scoreError']
    bench = row['benchmark']
    splitted = bench.split('.')
    clsname = splitted[-2]
    query = clsname[11:]
    variant_name = splitted[-3]
    option = s2soptions.from_variant_name(variant_name)
    config_str = option.compact_str()
    return CSVItem(query, time, err, config_str)


def groupvarsize_to_item(row):
    from s2soptions import reverse_rename
    time, err = row['primaryMetric']['score'], row['primaryMetric']['scoreError']
    methodname = row['benchmark'].split('.')[-1]
    par_version_short, query_type = methodname.split('_')

    mod = row['params']['mod']
    size = row['params']['numOrders']
    query = '_'.join([query_type, mod, size])

    par_option = reverse_rename(par_version_short)
    option = s2soptions.stream_baseline.with_option('multi_threading', par_option)
    config_str = option.compact_str()
    return CSVItem(query, time, err, config_str)


def distinct_to_item(row):
    from s2soptions import reverse_rename
    time, err = row['primaryMetric']['score'], row['primaryMetric']['scoreError']
    par_version_short = row['benchmark'].split('.')[-1]

    size = int(row['params']['nDistinct'])
    par_option = reverse_rename(par_version_short)
    return {'size':size, 'impl':par_option, 'time':time, 'err':err}


if __name__ == '__main__':

    from os import path, chdir

    # Convert JMH result JSON files into pandas dataframes
    this_dir = path.dirname(path.realpath(__file__))
    chdir(this_dir)
    results_dir = path.join(this_dir, 'results')

    def tpch_to_df(in_file):
        return json_to_df(path.join(results_dir, in_file), convert_func=tpch_to_item)

    def groupvarsize_to_df(in_file):
        df = json_to_df(path.join(results_dir, in_file), convert_func=groupvarsize_to_item)
        df['multi_threading'] = df['option_str'].apply(lambda x: x.split('_')[2])
        return df

    def distinct_to_df(in_file):
        return json_to_df(path.join(results_dir, in_file), convert_func=distinct_to_item)


    df_seq_jdk = tpch_to_df('sequential-jdk.json')
    df_seq_graalvm = tpch_to_df('sequential-graalvm.json')
    df_par_jdk = tpch_to_df('parallel-jdk.json')
    df_par_graalvm = tpch_to_df('parallel-graalvm.json')



    # Generate LaTex tables
    import gentables
    gentables.table_options(df_seq_jdk, 'out/table-options-jdk23.tex')
    gentables.table_options(df_seq_graalvm, 'out/table-options-graalvm23.tex')

    gentables.table_parallel(df_par_jdk, 'out/table-parallel-jdk.tex')
    gentables.table_parallel(df_par_graalvm, 'out/table-parallel-graalvm.tex')

    gentables.table_vs_imperative(df_seq_jdk, 'out/table-vsimperative-jdk23.tex')
    gentables.table_vs_imperative(df_seq_graalvm, 'out/table-vsimperative-graalvm23.tex')
    print('Tables generated')

    # Generate figures
    import genfigures
    df_micro_rq2_small = groupvarsize_to_df('microbenchmark-o2-small.json')
    df_micro_rq2_large = groupvarsize_to_df('microbenchmark-o2-large.json')
    genfigures.vargroupsize(df_micro_rq2_small, 'out/vargroupsize-small.png', desired_range=(1, 500))
    genfigures.vargroupsize(df_micro_rq2_large, 'out/vargroupsize-large.png', desired_range=(500, 500000))

    df_micro_distinct_small = distinct_to_df('microbenchmark-distinct-small.json')
    df_micro_distinct_large = distinct_to_df('microbenchmark-distinct-large.json')
    genfigures.distinct(df_micro_distinct_small, f'out/distinct-small.png', desired_range=(1, 1000))
    genfigures.distinct(df_micro_distinct_large, f'out/distinct-large.png', desired_range=(1000, 1000000))
    print('Figures generated')

    # Print dataframe for micro-RQ1 (in the paper, results are only in the text)
    df_micro_rq1 = tpch_to_df('microbenchmark-o1.json')
    print('Microbenchmark - RQ1')
    print(df_micro_rq1)

import json
import os
import subprocess
from datetime import datetime
from decimal import Decimal

from core.global_defs import tables_folder, pretty_triplestore_label, pretty_dataset_label
from core import global_defs

import pandas as pd

os.makedirs(tables_folder, exist_ok=True)


# Function to create plots for insert and delete performance
def extract_results():
    get_sub_dirs = lambda parent: sorted(str(s) for s in os.listdir(f'{parent}'))
    dataset_dirs = get_sub_dirs('benchmark_workspace/results/')

    all_update_results = []
    all_loading_results = []

    for dataset_dir in dataset_dirs:
        if dataset_dir not in global_defs.datasets:
            continue
        triplestore_dirs = get_sub_dirs(f'benchmark_workspace/results/{dataset_dir}')
        for triplestore_dir in triplestore_dirs:
            if triplestore_dir not in global_defs.triplestores:
                continue
            changesets_dirs = get_sub_dirs(f'benchmark_workspace/results/{dataset_dir}/{triplestore_dir}')
            changesets_dirs = list(filter(lambda d: d.startswith('changeset'), changesets_dirs))

            get_latest_timestamp_subdir = lambda path: max(get_sub_dirs(path), key=datetime.fromisoformat)

            for changeset_dir in changesets_dirs:
                # take the latest results

                latest_results_timestamp = get_latest_timestamp_subdir(
                    f'benchmark_workspace/results/{dataset_dir}/{triplestore_dir}/{changeset_dir}')

                update_results_path = f'benchmark_workspace/results/{dataset_dir}/{triplestore_dir}/{changeset_dir}/{latest_results_timestamp}/results.csv'
                try:
                    update_results = pd.read_csv(update_results_path)
                    update_results['triplestore'] = pretty_triplestore_label(triplestore_dir)
                    update_results['dataset'] = pretty_dataset_label(dataset_dir)
                    update_results['changeset'] = changeset_dir
                    update_results['update_type'] = update_results['query_id'].apply(
                        lambda x: 'insert' if x % 2 == 0 else 'delete')
                    all_update_results.append(update_results)
                except pd.errors.EmptyDataError:
                    continue

            latest_results_timestamp = get_latest_timestamp_subdir(
                f'benchmark_workspace/results/{dataset_dir}/{triplestore_dir}/loading')
            loading_results_path = f'benchmark_workspace/results/{dataset_dir}/{triplestore_dir}/loading/{latest_results_timestamp}/loading_stats.json'
            with open(loading_results_path, 'r') as f:
                loading_stats = json.load(f)
            loading_time = (Decimal(loading_stats['ns']) / 10 ** 9)

            # note: the memory consumption was actually measured in KB not bytes
            if triplestore_dir in ['tentris-baseline', 'tentris-insdel']:
                # Divide by 2 because the measuring method measures the copy-on-write snapshot as a full copy
                memory_consumptions = (Decimal(loading_stats['bytes']) / 10 ** 6 / 2)
            else:
                memory_consumptions = (Decimal(loading_stats['bytes']) / 10 ** 6)
            storage_gb = float(memory_consumptions)
            loading_duration_s = float(loading_time)
            dataset_size = {'wikidata': 5448662102, 'dbpedia': 681234276, 'swdf': 304592, 'bsbm': 99891833}[dataset_dir]
            all_loading_results.append({
                'triplestore': pretty_triplestore_label(triplestore_dir),
                'dataset': pretty_dataset_label(dataset_dir),
                'storage_gb': storage_gb,
                'loading_duration_s': loading_duration_s,
                'loading_duration_min': loading_duration_s / 60,
                'bytes/triple': storage_gb * 10 ** 9 / dataset_size,
                'triple/sec': dataset_size / loading_duration_s
            })
    combined_update_data = pd.concat(all_update_results, ignore_index=True)
    combined_loading_data = pd.DataFrame(all_loading_results)
    combined_update_data.to_csv(f'{tables_folder}/update_results.csv', index=False)
    combined_loading_data.to_csv(f'{tables_folder}/loading_results.csv', index=False)

    return combined_update_data, combined_loading_data


def aggregate_results(combined_update_data):
    # Aggregate results
    data = combined_update_data.copy()

    # Replace infinities only in runtime_secs column
    data['runtime_secs'] = data['runtime_secs'].replace([float('inf'), -float('inf')], float('nan'))

    aggregated_update_data = data.groupby(['dataset', 'triplestore', 'changeset'], dropna=True).agg(
        {'runtime_secs': ['mean', 'std', 'sum'], 'error': ['count']}).reset_index()
    aggregated_update_data.columns = ['dataset', 'triplestore', 'changeset',
                                      'runtime_secs_mean', 'runtime_secs_std', 'runtime_secs_sum',
                                      'error_count']
    aggregated_update_data.to_csv(f'{tables_folder}/aggregated_update_results.csv', index=False)
    return aggregated_update_data


def wikidata_scaling(aggregated_update_data):
    # Wikidata scaling
    wikidata_scaling = aggregated_update_data[aggregated_update_data['dataset'] == 'Wikidata'].copy()
    # The number behind 'changset' is the number of triples in the changeset
    wikidata_scaling['update_size'] = wikidata_scaling['changeset'].apply(lambda x: int(x.split('changesets')[-1]))
    wikidata_scaling['runtime_secs_per_triple'] = wikidata_scaling['runtime_secs_mean'] / wikidata_scaling[
        'update_size']
    wikidata_scaling.to_csv(f'{tables_folder}/wikidata_update_scaling_results.csv', index=False)


def dbpedia_results(aggregated_update_data):
    dbpedia = aggregated_update_data[aggregated_update_data['dataset'] == 'DBpedia'].copy()

    dbpedia.to_csv(f'{tables_folder}/dbpedia_update_results.csv', index=False)

def bsbm_results(aggregated_update_data):
    bsbm = aggregated_update_data[aggregated_update_data['dataset'] == 'BSBM'].copy()

    bsbm.to_csv(f'{tables_folder}/bsbm_update_results.csv', index=False)


combined_update_data, _ = extract_results()
aggregated_update_data = aggregate_results(combined_update_data)
wikidata_scaling(aggregated_update_data)

dbpedia_results(aggregated_update_data)

# run count_size.sh
if not os.path.exists(f'{tables_folder}/dbpedia_update_sizes.csv'):
    print("Counting DBpedia updates. Might get downloaded if not yet in benchmark_workspace. I hope you are not on mobile data. Go, get a tea or coffee. Don't spill it.")
    subprocess.run(['bash', 'core/extract_dbpedia_update_sizes.sh'])

# if not os.path.exists(f'{tables_folder}/bsbm_update_sizes.csv'):
#     print('Counting BSBM updates. Might get downloaded if not yet in benchmark_workspace.')
#     subprocess.run(['bash', 'core/extract_bsbm_update_sizes.sh'])
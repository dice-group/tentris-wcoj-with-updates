from plot_commons import *
from core.global_defs import tables_folder, triplestore_order, pretty_dataset_label

import pandas as pd


def scatter_plot(dataset):
    pretty_dataset = pretty_dataset_label(dataset)
    fig = plt.figure(figsize=figsize_cm(y=10))
    gs = fig.add_gridspec(40, 30)
    ax1 = fig.add_subplot(gs[0:22, :])
    ax2 = fig.add_subplot(gs[30:40, 0:14])
    ax3 = fig.add_subplot(gs[30:40, 15:22])
    ax4 = fig.add_subplot(gs[30:40, 24:29])

    # Plot 1: Scaling Performance
    update_sizes = pd.read_csv(f'{tables_folder}/{dataset}_update_sizes.csv')
    update_results = pd.read_csv(f'{tables_folder}/update_results.csv')

    update_results = update_results[update_results['dataset'] == pretty_dataset]
    merged_data = pd.merge(update_sizes, update_results, on='query_id')
    merged_data['runtime_secs_per_triple'] = merged_data['runtime_secs'] / merged_data['update_size_triples']

    sns.scatterplot(ax=ax1, data=merged_data,
                    x='update_size_triples',
                    y='runtime_secs_per_triple',
                    hue='triplestore',
                    palette=colors_for_triplestores(triplestore_order),
                    hue_order=triplestore_order,
                    s=3,
                    alpha=0.5)
    if dataset == 'dbpedia':
        ax1.set_xscale('log')

    ax1.set_yscale('log')
    ax1.set_xlabel('update size (triples)'), ax1.set_ylabel('seconds/triple update ◀')
    leg = ax1.legend(fontsize='x-small', ncol=2, markerscale=2)
    for lh in leg.legend_handles: lh.set_alpha(1)

    # Plot 2: Boxplot of runtime

    sns.boxplot(ax=ax2, data=update_results,
                x='runtime_secs',
                y='triplestore',
                palette=colors_for_triplestores(triplestore_order),
                hue='triplestore',
                hue_order=triplestore_order,
                order=triplestore_order,
                showmeans=True,
                meanprops={"marker": "X", "markerfacecolor": "black", "markeredgecolor": 'none', "markersize": 5},
                flierprops={"marker": "o", "markersize": 3, "alpha": .5},
                linewidth=0.5)
    ax2.set_xscale('log')
    ax2.set_xlabel('seconds ◀')
    ax2.set_title('Runtime')

    # Plot 3: Total runtime
    summary_data = pd.read_csv(f'{tables_folder}/aggregated_update_results.csv')
    summary_data = summary_data[summary_data['dataset'] == pretty_dataset]
    sns.barplot(ax=ax3, data=summary_data,
                x='runtime_secs_sum',
                y='triplestore', hue='triplestore', legend=False,
                palette=colors_for_triplestores(triplestore_order),
                order=triplestore_order, hue_order=triplestore_order
                )
    ax3.set_title('Total Runtime')
    ax3.set_xlabel('seconds ◀'), ax3.set_ylabel(None)
    ax3.set_yticks([])

    # Plot 4: Failed updates
    sns.barplot(ax=ax4, data=summary_data,
                x='error_count',
                y='triplestore', hue='triplestore', legend=False,
                palette=colors_for_triplestores(triplestore_order),
                order=triplestore_order, hue_order=triplestore_order
                )
    ax4.set_title('Failed')
    ax4.set_xlabel('#updates ◀'), ax4.set_ylabel(None)
    ax4.set_xticks([0, update_results['query_id'].max() + 1]), ax4.set_yticks([])

    save_plot(f"{dataset}-scaling-and-summary")


def wikidata_plot():
    fig = plt.figure(figsize=figsize_cm(y=10))
    gs = fig.add_gridspec(40, 30)
    ax1 = fig.add_subplot(gs[0:22, :])
    ax2 = fig.add_subplot(gs[30:40, 0:14])
    ax3 = fig.add_subplot(gs[30:40, 15:22])
    ax4 = fig.add_subplot(gs[30:40, 24:29])

    # Plot 1: Scaling Performance
    scaling_data = pd.read_csv(f'{tables_folder}/wikidata_update_scaling_results.csv')
    update_results = pd.read_csv(f'{tables_folder}/update_results.csv')
    update_results = update_results[update_results['dataset'] == 'Wikidata']
    scaling_data['runtime_secs_per_triple_std'] = scaling_data['runtime_secs_std'] / scaling_data['update_size']

    for triplestore in triplestore_order:
        triplestore_data = scaling_data[scaling_data['triplestore'] == triplestore]

        ax1.fill_between(x=triplestore_data['update_size'],
                         y1=(triplestore_data['runtime_secs_mean'] - triplestore_data['runtime_secs_std']) /
                            triplestore_data['update_size'],
                         y2=(triplestore_data['runtime_secs_mean'] + triplestore_data['runtime_secs_std']) /
                            triplestore_data['update_size'],
                         alpha=0.1, color=ts2color[triplestore], label='_nolegend_')
    sns.lineplot(ax=ax1, data=scaling_data,
                 x='update_size',
                 y='runtime_secs_per_triple', hue='triplestore',
                 palette=colors_for_triplestores(triplestore_order),
                 hue_order=triplestore_order)
    leg = ax1.legend(fontsize='x-small', ncol=2, markerscale=2)
    ax1.set_xscale('log', subs=[]), ax1.set_yscale('log')
    ax1.set_xlabel('update size (triples)'), ax1.set_ylabel('seconds/triple update ◀')

    # Plot 2: Boxplot of runtime
    update_results = update_results[update_results['changeset'] == 'changesets10000']
    sns.boxplot(ax=ax2, data=update_results, #[update_results['triplestore'] != "QLever"],
                x='runtime_secs',
                y='triplestore',
                palette=colors_for_triplestores(triplestore_order),
                hue='triplestore',
                hue_order=triplestore_order,
                order=triplestore_order,
                showmeans=True,
                meanprops={"marker": "X", "markerfacecolor": "black", "markeredgecolor": 'none', "markersize": 5},
                flierprops={"marker": "o", "markersize": 3, "alpha": .5},
                linewidth=0.5)
    ax2.set_xscale('log')
    ax2.set_xlabel('seconds ◀')
    ax2.set_title('Runtime')
    ax2.text(-.375, 1.09, 'update size: 10', transform=ax2.transAxes, fontsize=BIGGER_SIZE, fontweight='bold',
             va='bottom', ha='left')
    ax2.text(.184, 1.178, '4', transform=ax2.transAxes, fontsize=5, fontweight='bold',
             va='bottom', ha='left')

    # Plot 3: Total runtime
    summary_data = pd.read_csv(f'{tables_folder}/aggregated_update_results.csv')
    summary_data = summary_data[
        (summary_data['dataset'] == 'Wikidata') & (summary_data['changeset'] == 'changesets10000')]
    sns.barplot(ax=ax3, data=summary_data,
                x='runtime_secs_sum',
                y='triplestore', hue='triplestore', legend=False,
                palette=colors_for_triplestores(triplestore_order),
                order=triplestore_order, hue_order=triplestore_order
                )
    ax3.set_title('Total Runtime')
    ax3.set_xlabel('seconds ◀'), ax3.set_ylabel(None)
    ax3.set_yticks([])

    # Plot 4: Failed updates
    sns.barplot(ax=ax4, data=summary_data,
                x='error_count',
                y='triplestore', hue='triplestore', legend=False,
                palette=colors_for_triplestores(triplestore_order),
                order=triplestore_order, hue_order=triplestore_order
                )
    ax4.set_title('Failed')
    ax4.set_xlabel('#updates ◀'), ax4.set_ylabel(None)
    ax4.set_xticks([0, update_results['query_id'].max() + 1]), ax4.set_yticks([])

    save_plot("wikidata-scaling-and-summary")


def update_size_triples_hist():
    dbpedia_update_sizes = pd.read_csv(f'{tables_folder}/dbpedia_update_sizes.csv')

    plt.figure(figsize=fig_size)
    bins = [m * 10 ** e for e in range(5) for m in range(1, 10)]

    sns.histplot(dbpedia_update_sizes['update_size_triples'], bins=bins, kde=False, color='grey')
    plt.xscale('log')
    plt.xlabel('Update Size (triples)')
    plt.ylabel('Frequency')
    plt.title('Histogram of Update Size Triples in DBpedia')
    save_plot("dbpedia-update-size-triples-histogram")


wikidata_plot()
scatter_plot('dbpedia')
# scatter_plot('bsbm')

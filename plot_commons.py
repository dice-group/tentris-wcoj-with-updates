import os
import subprocess

import matplotlib.pyplot as plt

import seaborn as sns

from core.global_defs import triplestores, pretty_triplestore_label

SMALL_SIZE = 6
MEDIUM_SIZE = 8
BIGGER_SIZE = 8

plt.rc('font', size=MEDIUM_SIZE)  # controls default text sizes
plt.rc('axes', titlesize=BIGGER_SIZE, titleweight='bold')  # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
plt.rc('xtick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
plt.rc('ytick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)  # legend fontsize

figure_folder = 'figures'
os.makedirs(figure_folder, exist_ok=True)

colors = sns.color_palette("colorblind6", len(triplestores))
ts2color = {ts: colors[i] for i, ts in enumerate(triplestores)}
# exchange the colors of tentris-insdel and oxigraph
ts2color['tentris-insdel'], ts2color['oxigraph'] = ts2color['oxigraph'], ts2color['tentris-insdel']
for ts in triplestores:
    ts2color[pretty_triplestore_label(ts)] = ts2color[ts]


def colors_for_triplestores(tss):
    return [ts2color[ts] for ts in tss]


def crop(pdf_path):
    try:
        subprocess.run(['pdfcrop', pdf_path, pdf_path])
    except:
        pass


for ts in list(ts2color.keys()):
    ts2color[pretty_triplestore_label(ts)] = ts2color[ts]


def save_plot(file_stem):
    pdf = f"{figure_folder}/{file_stem}.pdf"
    # png = f"{output_folder}/{file_stem}.png"
    # svg = f"{output_folder}/{file_stem}.svg"
    plt.savefig(pdf, bbox_inches='tight'), crop(pdf)
    # plt.savefig(png, bbox_inches='tight', dpi=300)
    # plt.savefig(svg, bbox_inches='tight')


fig_x, fig_y = 12.2 / 2.54, 7 / 2.54


def figsize_cm(y=5, x=12.2):
    return x / 2.54, y / 2.54


fig_size = (fig_x, fig_y)

import pandas as pd
import numpy as np

import pandas as pd
from matplotlib.ticker import ScalarFormatter

data1 = [
    ['000001.SZ','2016-06-24',0.165260,0.002198,0.085632,-0.078074,0.173832,0.214377,0.068445],
    ['000001.SZ','2016-06-27',0.165537,0.003583,0.063299,-0.048674,0.180890,0.202724,0.081748],
    ['000001.SZ','2016-06-28',0.135215,0.010403,0.059038,-0.034879,0.111691,0.122554,0.042489],
    ['000002.SH','2016-06-24',0.068774,0.019848,0.058476,-0.049971,0.042805,0.053339,0.079592],
    ['000002.SH','2016-06-27',0.039431,0.012271,0.037432,-0.027272,0.010902,0.077293,-0.050667],
    ['000002.SH','2016-06-28',0.039431,0.012271,0.037432,-0.027272,0.010902,0.077293,-0.050667],
    ['000002.SH','2016-06-29',0.039431,0.012271,0.037432,-0.027272,0.010902,0.077293,-0.050667]
]
df1 = pd.DataFrame(data1,columns=["code","datetime","BP","CFP","EP","ILLIQUIDITY","REVS20","SRMI","VOL20"])
df1 = df1.set_index(["code","datetime"])

import matplotlib.cm as cm
from mfm_learner.utils import utils
import matplotlib.pyplot as plt


def plot_multiple():
    f, axes = plt.subplots(len(df1.columns), 1, figsize=(18, 6))
    for icol, name in enumerate(df1.columns):
        ax = axes[icol]
        data = df1[name]
        data = data.loc[:, ::-1]  # we want negative quantiles as 'red'
        ax.legend()
        data.plot(lw=2, ax=ax, cmap=cm.coolwarm)
        ymin, ymax = data.min().min(), data.max().max()
        ax.set(ylabel='Log Cumulative Returns',
               title='''Cumulative Return by Quantile
                        ({} Period Forward Return) for factor {}'''.format(icol, 'factor_name'),
               xlabel='',
               yscale='symlog',
               yticks=np.linspace(ymin, ymax, 5),
               ylim=(ymin, ymax))

        ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.axhline(1.0, linestyle='-', color='black', lw=1)
        icol += 1
    plt.show()


def plot_in_one():
    #f, axes = plt.subplots(len(df1.columns), 1, figsize=(18, 6))

    color = cm.rainbow(np.linspace(0, 1, len(df1.columns)))
    for icol, name in enumerate(df1.columns):
        data = df1[name]
        data = data.loc[:, ::-1]  # we want negative quantiles as 'red'
        plt.legend()
        ymin, ymax = data.min().min(), data.max().max()
        plt.ylabel='Log Cumulative Returns'
        plt.title='Cumulative Return by Quantile({} Period Forward Return) for factor {}'''.format(icol, 'factor_name')
        plt.xlabel=''
        plt.yscale='symlog'
        plt.yticks=np.linspace(ymin, ymax, 5)
        plt.ylim=(ymin, ymax)
        # plt.axis.set_major_formatter(ScalarFormatter())
        # plt.axhline(1.0, linestyle='-', color='black', lw=1)

        data.plot(c=color[icol])

    plt.show()

plot_in_one()
# python -m test.test_plot

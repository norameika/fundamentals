# coding:utf-8
import pylab as plt
import numpy as np


class chart:
    def __init__(self, x=1, y=1, ws=0.1, **kwargs):
        self.colors = ('#2980b9', '#e74c3c', '#27ae60', '#f1c40f', '#8e44ad', '#e67e22', '#bc8f8f', '#2c3e50')
        if 'aspect_ratio' in kwargs.keys():
            aspect_ratio = kwargs['aspect']
        else:
            aspect_ratio = (16, 9)
        self.fig = plt.figure(figsize=aspect_ratio, dpi=100, facecolor='w', edgecolor='k')
        self.axis = list()
        self.x = x
        self.y = y

        for i in range(x * y):
            if 'd3' in kwargs.keys():
                if i in kwargs['d3']:
                    self.axis.append(self.fig.add_subplot(y, x, i + 1, axisbg='white', projection='3d'))
            else:
                self.axis.append(self.fig.add_subplot(y, x, i + 1, axisbg='white'))
            self.fig.subplots_adjust(wspace=0.2)
            self.fig.subplots_adjust(hspace=0.4)

    def set_xlim(self, xrange, axisid=0):
        axis = self.axis[axisid]
        axis.set_xlim(xrange)

    def set_ylim(self, yrange, axisid=0):
        axis = self.axis[axisid]
        axis.set_ylim(xrange)

    def bar_chart(self, data, axis_id=0, lw=0.77, alpha=1, *args, **kwargs):
        self.data.update({axis_id: data})
        ax = self.axis[axis_id]
        colors = self.colors
        buf = None
        if 'twinx' in kwargs.keys():
            ax = ax.twinx()
        if 'fs' in kwargs.keys():
            fs = kwargs['fs']
        else:
            fs = 13
        if 'title' in kwargs.keys():
            ax.set_title(kwargs['title'])
        if 'xlabel' in kwargs.keys():
            ax.set_xlabel(kwargs['xlabel'])
        if 'ylabel' in kwargs.keys():
            ax.set_ylabel(kwargs['ylabel'])

        if 'legend' not in kwargs.keys():
            label = range(len(data))
        else:
            label = kwargs['legend']

        for cnt, (d, c, l) in enumerate(zip(data, colors, label)):
            if cnt == 0:
                ax.bar(d[0], d[1], color=c, alpha=1, linewidth=lw, label=l)
                buf = d[1]
            else:
                ax.bar(d[0], d[1], color=c, alpha=1, linewidth=lw, bottom=buf, label=l)
                buf = (np.array(buf) + np.array(d[1])).tolist()

        if 'legend' in kwargs.keys():
            if 'loc' in kwargs.keys():
                ax.legend(fontsize=fs, loc=kwargs['loc'])
            else:
                ax.legend(fontsize=fs, loc='upper left')

        plt.legend(fontsize=fs)
        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(fs)
        self.canvas.draw()

    def scatter(self, data, axis_id=0, maker='-o', lw=0.77, alpha=1, *args, **kwargs):
        ax = self.axis[axis_id]
        if 'twinx' in kwargs.keys():
            ax = ax.twinx()
        if 'fs' in kwargs.keys():
            fs = kwargs['fs']
        else:
            fs = 13
        if 'title' in kwargs.keys():
            ax.set_title(kwargs['title'])
        if 'xlabel' in kwargs.keys():
            ax.set_xlabel(kwargs['xlabel'])
        if 'ylabel' in kwargs.keys():
            ax.set_ylabel(kwargs['ylabel'])

        if 'legend' not in kwargs.keys():
            label = range(len(data))
        else:
            label = kwargs['legend']

        if 'ms' not in kwargs.keys():
            ms = 10
        else:
            ms = kwargs['ms']

        for d, c, l in zip(data, self.colors, label):
            ax.plot(d[0], d[1], maker, ms=ms, color=c, alpha=alpha, linewidth=lw, label=l)

        if 'xtick' in kwargs.keys():
            if 'rot' in kwargs.keys():
                rot = kwargs['rot']
            else:
                rot = 0
            xtick = kwargs['xtick']
            ax.xaxis.set_ticks([int(i) for i in data[0][0]])
            ax.set_xticklabels(xtick, rotation=rot)

        if 'legend' in kwargs.keys():
            if 'loc' in kwargs.keys():
                ax.legend(fontsize=fs, loc=kwargs['loc'])
            else:
                ax.legend(fontsize=fs, loc='upper left')

        for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
            item.set_fontsize(fs)

    def draw(self):
        # plt.tight_layout()
        plt.show()


def is_num(num):
    try:
        int(num)
        return 1
    except:
        try:
            float(num)
            return 1
        except:
            return 0
        return 0
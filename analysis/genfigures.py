
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


from s2soptions import rename


palette = sns.color_palette()
line_style = {
    'SEQ': (palette[0], (3, 1)),
    'P': (palette[1], (1, 0)),
    'PU': (palette[2], (1, 2)),
    'CG': (palette[3], (1, 1)),
    'CGCC': (palette[4], (3, 3))
}


def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)

def thousands_to_k(s):
    return rreplace(s, '000', 'K', 1)


def vargroupsize(df, outname, desired_range=(1, None)):

    df['mod'] = df['query'].apply(lambda q: int(q.split('_')[1]))
    df['size'] = df['query'].apply(lambda q: int(q.split('_')[2]))
    df['name'] = df['multi_threading'].apply(rename)
    df['query_name'] = df['query'].apply(lambda q: q.split('_')[0])

    if len(df['size'].unique()) > 1:
        raise ValueError('currently one size is supported ' + str(df['size'].unique()) + ' - ' + str(len(df['size'].unique())))

    names = df['query_name'].unique()
    for name in names:
        tmp_df = df[df['query_name'] == name]
    
        tmp_df = tmp_df[['name', 'mod', 'time', 'err']]
        tmp_df = tmp_df.sort_values(by=['name', 'mod'])
        implementations = tmp_df['name'].unique()

        plt.figure(figsize=(10, 8))

        sns.lineplot(data=tmp_df, 
                    x='mod', y='time', 
                    hue='name', 
                    linewidth=10,
                    style='name',
                    markers=False,
                    dashes={i: line_style[i][1] for i in implementations},
                    palette=[line_style[i][0] for i in implementations])

        start_x_tick, end_x_tick = desired_range
        ticks = plt.xticks()[0] 
        ticks = [(start_x_tick if t == 0 else t) for t in ticks if t >= 0]  
        if end_x_tick:
            while ticks[-1] > end_x_tick: 
                ticks = ticks[:-1]

        tick_labels = [thousands_to_k(str(int(t))) for t in ticks]
        ticksize = 28
        plt.xticks(ticks, labels=tick_labels, fontsize=ticksize, fontweight="bold") 
        plt.yticks(fontsize=ticksize, fontweight="bold")  

        plt.xlabel('')
        plt.ylabel('')
        plt.title('')
        plt.grid(True)

        # Apply log scale
        plt.yscale("log")

        # --- Generate Legend Handles Correctly ---
        legend_handles = [
            mlines.Line2D([], [],
                          label=i,
                          color=line_style[i][0],
                          linestyle=(0, line_style[i][1]),
                          linewidth=5)
            for i in implementations
        ]

        # Remove legend from main plot
        plt.legend().remove()  

        # Save main plot
        plt.savefig(outname.replace('png', name+'.png'), dpi=300, bbox_inches='tight')
        plt.close()

        # --- Create separate figure for legend ---
        fig_legend, ax_legend = plt.subplots(figsize=(5, 0.3))  # Horizontal layout
        ax_legend.axis("off")  # Hide axes

        # Create horizontal legend
        ax_legend.legend(legend_handles,
                         implementations,
                         loc="center",
                         ncol=len(implementations),
                         fontsize=14,
                         frameon=False,
                         handletextpad=0.5,      # spacing between handle and label
                         columnspacing=1.0,      # spacing between columns
                         borderaxespad=0.0       # spacing between legend and axes
                         )

        # Save the legend separately
        fig_legend.tight_layout(pad=0.0)
        fig_legend.savefig('out/legend_vargroupsize.png', dpi=300, bbox_inches='tight')
        plt.close()



def distinct(df, outname, desired_range=(1, None)):

    plt.figure(figsize=(10, 8))

    implementations = df['impl'].unique()

    sns.lineplot(data=df,
                 x='size', y='time',
                 hue='impl',
                 linewidth=10,
                 style='impl',
                 markers=False,
                 dashes={i: line_style[i][1] for i in implementations},
                 palette=[line_style[i][0] for i in implementations])




    start_x_tick, end_x_tick = desired_range
    ticks = plt.xticks()[0]
    ticks = [(start_x_tick if t == 0 else t) for t in ticks if t >= 0]
    if end_x_tick:
        while ticks[-1] > end_x_tick:
            ticks = ticks[:-1]

    tick_labels = [thousands_to_k(str(int(t))) for t in ticks]
    ticksize = 28
    plt.xticks(ticks, labels=tick_labels, fontsize=ticksize, fontweight="bold")
    plt.yticks(fontsize=ticksize, fontweight="bold")
    plt.xlabel('')
    plt.ylabel('')
    plt.title('')
    plt.grid(True)

    # --- Generate Legend Handles Correctly ---
    legend_handles = [
        mlines.Line2D([], [],
                      label=i,
                      color=line_style[i][0],
                      linestyle=(0, line_style[i][1]),
                      linewidth=5)
        for i in implementations
    ]


    plt.legend().remove()
    plt.subplots_adjust(top=0.95)
    plt.savefig(outname, dpi=300, pad_inches=0)
    plt.close()

    # --- Create separate figure for legend ---
    fig_legend, ax_legend = plt.subplots(figsize=(5, 0.3))  # Horizontal layout
    ax_legend.axis("off")  # Hide axes

    # Create horizontal legend
    ax_legend.legend(
        legend_handles,
        implementations,
        loc="center",
        ncol=len(implementations),
        fontsize=14,
        frameon=False,
        handletextpad=0.5,      # spacing between handle and label
        columnspacing=1.0,      # spacing between columns
        borderaxespad=0.0       # spacing between legend and axes
    )

    # Save the legend separately
    fig_legend.tight_layout(pad=0.0)
    fig_legend.savefig('out/legend_distinct.png', dpi=300, bbox_inches='tight')
    plt.close()

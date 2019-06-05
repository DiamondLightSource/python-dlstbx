from __future__ import absolute_import, division, print_function

import os

import tempfile
from math import ceil
import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import networkx as nx
from dlstbx.util.radar_plot import radar_factory


def parse_fastep_table(root_wd):
    res = {"CCall": {}, "CCweak": {}, "CCres": {}, "CFOM": {}, "No. found": {}}
    rmrk = lambda s: float(s.split("|")[0])
    num_set = set([])
    use_cc_res = False
    logfile = os.path.join(root_wd, "fast_ep.log")
    with open(logfile) as f:
        for line in f:
            if "Spacegroup:" in line:
                cc_all_dict = {}
                cc_weak_dict = {}
                cc_res_dict = {}
                cfom_dict = {}
                fnum_dict = {}

                sg = line.split()[1]
                if "best" in line:
                    best_sg = sg
                continue
            if "CCall" in line and "Res." in line:
                if "CCres" in line:
                    use_cc_res = True
                for data_line in f:
                    try:
                        parsed_line = data_line.split()
                        tokens = len(parsed_line)
                        if "best" in data_line:
                            tokens -= 1
                        try:
                            if use_cc_res:
                                num, resol, cc_res, _, cc_all, _, cc_weak, _, cfom, _, fnum = parsed_line[
                                    :tokens
                                ]
                            else:
                                num, resol, cc_all, _, cc_weak, _, cfom, _, fnum = parsed_line[
                                    :tokens
                                ]
                            num_set.add(int(num))
                        except ValueError:
                            if use_cc_res:
                                resol, cc_res, _, cc_all, _, cc_weak, _, cfom, _, fnum = parsed_line[
                                    :tokens
                                ]
                            else:
                                resol, cc_all, _, cc_weak, _, cfom, _, fnum = parsed_line[
                                    :tokens
                                ]
                        for dct, val in zip(
                            [cc_all_dict, cc_weak_dict, cfom_dict],
                            [cc_all, cc_weak, cfom],
                        ):
                            try:
                                dct[resol].append(rmrk(val))
                            except KeyError:
                                dct[resol] = [rmrk(val)]
                        try:
                            fnum_dict[resol].append(int(fnum))
                        except KeyError:
                            fnum_dict[resol] = [int(fnum)]
                        if use_cc_res:
                            try:
                                cc_res_dict[resol].append(rmrk(cc_res))
                            except KeyError:
                                cc_res_dict[resol] = [rmrk(cc_res)]
                        if "best" in data_line:
                            best_num = int(num)
                            best_fnum = int(fnum)
                    except Exception:
                        res["CCall"][sg] = cc_all_dict
                        res["CCweak"][sg] = cc_weak_dict
                        res["CCres"][sg] = cc_res_dict
                        res["CFOM"][sg] = cfom_dict
                        res["No. found"][sg] = fnum_dict
                        if "Time" in data_line:
                            break
                        if "Spacegroup" in data_line:
                            cc_all_dict = {}
                            cc_weak_dict = {}
                            cc_res_dict = {}
                            cfom_dict = {}
                            fnum_dict = {}

                            sg = data_line.split()[1]
                            if "best" in data_line:
                                best_sg = sg
                            break
    num_list = sorted(list(num_set))

    return num_list, res, (best_num, best_fnum, best_sg)


def fastep_radar_plot(tmpl_data, nums, data, best_vals):
    best_num, best_fnum, best_sg = best_vals
    fig = plt.figure(figsize=(18, 6), facecolor="w")
    # fig.subplots_adjust(wspace=0.25, hspace=0.20, top=0.85, bottom=0.05)

    colors = ["b", "r", "g", "m", "y", "c", "brown", "indigo"]
    ks = ["CCall", "CCweak", "CCres", "CFOM"]
    n_plots = len(ks)
    for n, case_data in enumerate((p, data[p]) for p in ks):
        title = case_data[0]
        N = len(nums)
        theta = radar_factory(N, frame="circle")
        # Add extra point to fix fill_between
        theta = np.append(theta, 0)

        spoke_labels = [str(i) for i in nums]
        ax = fig.add_subplot(1, n_plots, n + 1, projection="radar")
        ax.set_title(
            title,
            size=20,
            position=(0.5, -0.2),
            horizontalalignment="center",
            verticalalignment="center",
        )
        for i, (d, color) in enumerate(zip(sorted(case_data[1].keys()), colors)):
            shells = case_data[1][d]
            resol = sorted(shells.keys(), reverse=True)
            if n == 0:
                if i == 0:
                    plt.figtext(
                        0.005, 0.85, "Spacegroups", ha="left", color="k", size=20
                    )
                    plt.figtext(
                        0.005, 0.35, "Resolutions", ha="left", color="k", size=20
                    )
                plt.figtext(0.025, 0.8 - 0.05 * i, d, ha="left", color=color, size=20)
            if d == best_sg:
                best_color = color
            for idx, k in enumerate(resol):
                v = shells[k]
                v.append(v[0])
                ax.plot(theta, v[: len(theta)], color=color, label=k)
                if n == 0:
                    plt.figtext(
                        0.025, 0.3 - 0.05 * idx, k, ha="left", color="k", size=20
                    )
                try:
                    ax.fill_between(
                        theta,
                        shells[k],
                        shells[resol[idx - 1]],
                        facecolor=color,
                        alpha=0.25,
                    )
                except Exception:
                    continue
        ax.set_varlabels(spoke_labels)
        ax.yaxis.set_ticklabels([])

    plt.figtext(
        0.5,
        0.95,
        "Best spacegroup:",
        ha="center",
        color="black",
        weight="bold",
        size=20,
    )
    plt.figtext(
        0.6,
        0.95,
        "{}".format(best_sg),
        ha="left",
        color=best_color,
        weight="bold",
        size=20,
    )
    plt.figtext(
        0.5,
        0.9,
        "Substructure atoms search / found :  {} / {}".format(best_num, best_fnum),
        ha="center",
        color="black",
        weight="bold",
        size=20,
    )
    temp = tempfile.NamedTemporaryFile(prefix="fastep_table", suffix=".png")
    plt.savefig(temp.name, transparent=True, bbox_inches="tight")
    plt.close()
    try:
        with open(temp.name, "rb") as f:
            img_data = f.read()
            tmpl_data["html_images"]["img_fastep_table"] = img_data
    except IOError:
        pass


def fastep_sites_plot(tmpl_data, num_list, fnum_data, best_fnum, best_sg):
    tmpl_data["img_fastep_sites"] = []
    for nplot, (sg, fnum_dict) in enumerate(fnum_data.iteritems()):
        fig = plt.figure(figsize=(9, 6), facecolor="w")
        fnum_set = set([])
        for v in fnum_dict.values():
            fnum_set.update(set(v))

        G = nx.DiGraph()
        labels = {}
        for n in num_list:
            lb = "o" + str(n)
            G.add_node(lb, label=n)
            labels[lb] = str(n)
        for n in fnum_set:
            lb = "d" + str(n)
            G.add_node(lb, label=n)
            labels[lb] = str(n)

        for resol, vals in fnum_dict.iteritems():
            for i, j in zip(num_list, vals):
                n1 = "o" + str(i)
                n2 = "d" + str(j)
                if (n1, n2) in G.edges():
                    G[n1][n2]["label"].append(resol)
                else:
                    G.add_edge(n1, n2, label=[resol])

        edges = G.edges()
        for a, b in edges:
            G[a][b]["label"] = " ".join(G[a][b]["label"])
        lb = dict([((a, b), G[a][b]["label"]) for a, b in edges])

        ax = fig.add_subplot(111)

        pos = nx.spring_layout(G, k=0.5)
        nx.draw_networkx_nodes(
            G,
            pos,
            ["o" + str(i) for i in num_list],
            node_size=1000,
            node_color="y",
            ax=ax,
        )
        if sg == best_sg:
            nx.draw_networkx_nodes(
                G,
                pos,
                ["d" + str(best_fnum)],
                node_shape="v",
                node_size=2000,
                node_color="c",
                ax=ax,
            )
        nx.draw_networkx_nodes(
            G,
            pos,
            ["d" + str(j) for j in fnum_set if j != int(best_fnum) or sg != best_sg],
            node_size=1000,
            node_color="r",
            ax=ax,
        )
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=20, ax=ax)
        nx.draw_networkx_edges(G, pos, edges=edges, ax=ax)
        nx.draw_networkx_edge_labels(G, pos, edge_labels=lb, font_size=14, ax=ax)

        ax.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)
        plt.axis("off")
        if sg == best_sg:
            plt.title("{0}".format(sg), fontsize=16, fontweight="bold")
        else:
            plt.title("{0}".format(sg), fontsize=14)

        temp = tempfile.NamedTemporaryFile(
            prefix="fastep_sites_{}".format(sg), suffix=".png"
        )
        plt.savefig(temp.name, transparent=True, bbox_inches="tight")
        plt.close()
        try:
            with open(temp.name, "rb") as f:
                img_data = f.read()
                img_name = "img_fastep_sites{}".format(sg)
                tmpl_data["img_fastep_sites"].append(img_name)
                tmpl_data["html_images"][img_name] = img_data

        except IOError:
            pass

    n_col = range(min(len(fnum_data), 3))
    n_row = range(int(ceil(len(fnum_data) / 3)))
    tmpl_data["img_fastep_sites"].append(img_name)
    tmpl_data["img_fastep_ncol"] = n_col
    tmpl_data["img_fastep_nrow"] = n_row

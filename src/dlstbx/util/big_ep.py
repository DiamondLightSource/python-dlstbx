from __future__ import annotations

import getpass
import glob
import json
import logging
import math
import os
import platform
import smtplib
import subprocess
import tempfile
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import matplotlib as mpl
import numpy as np
from iotbx import data_plots

from dlstbx.util.big_ep_helpers import get_map_model_from_json

mpl.use("Agg")
from smtplib import SMTPSenderRefused
from time import sleep

import matplotlib.pyplot as plt

params = {
    "axes.labelsize": 20,
    "font.size": 28,
    "legend.fontsize": 18,
    "legend.columnspacing": 0.5,
    "legend.scatterpoints": 1,
    "legend.handletextpad": 0.3,  # the space between the legend line and legend text in fraction of fontsize
    "axes.labelpad": 15.0,
    "xtick.labelsize": 18,
    "ytick.labelsize": 18,
    "text.usetex": False,
    "figure.figsize": [8, 6],
    "figure.subplot.left": 0.15,  # the left side of the subplots of the figure
    "figure.subplot.right": 0.85,  # the right side of the subplots of the figure
    "figure.subplot.bottom": 0.15,  # the bottom of the subplots of the figure
    "figure.subplot.top": 0.85,  # the top of the subplots of the figure
    "figure.subplot.wspace": 0.2,  # the amount of width reserved for blank space between subplots
    "figure.subplot.hspace": 0.2,  # the amount of height reserved for white space between subplots
}

mpl.rcParams.update(params)

logger = logging.getLogger("dlstbx.util.big_ep")


def read_xia2_processing(tmpl_data):
    def get_plot_data(logfile, table_name, graph_name):
        try:
            tables = data_plots.import_ccp4i_logfile(logfile)
        except ValueError:
            logger.exception("Error reading data from log file %s", logfile)
            return None
        # Find appropriate table
        for table in tables:
            if table_name not in table.title:
                continue
            try:
                # There seems to be some variation in plot names when anomalous data is fitted
                for tmp_name in table.graph_names:
                    if graph_name not in tmp_name:
                        continue
                    graph = table.get_graph(tmp_name)
                    plots = graph.get_plots()
                    plot_data = {}
                    # Load all plot data in the selected graph
                    x_label = "Resolution"
                    for plot, y_label in zip(plots, graph.y_labels):
                        plot_data.update({y_label: plot})
                    stat = {"plot_data": plot_data, "plot_axis": x_label}
                    return stat
            except Exception:
                logger.exception(
                    "Error reading table %s from %s", (table_name, logfile)
                )
                continue

    def get_autoPROC_stats_data(logfile, graph_names):
        try:
            with open(logfile) as fp:
                lines = iter(fp.readlines())
        except OSError:
            logger.exception("Error reading data from log file %s", logfile)
            return None
        for line in lines:
            print(line)
            if graph_names[0] in line:
                name_line = line.split()
                idx_dict = {n: name_line.index(n) + 2 for n in graph_names}
                data_dict = {n: [] for n in graph_names}
                res_data = []
                next(lines)
                try:
                    for data_line in lines:
                        data_vals = data_line.split()
                        res_data.append(1.0 / pow(float(data_vals[2]), 2))
                        for graph_name in graph_names:
                            tmp_val = float(data_vals[idx_dict[graph_name]])
                            data_dict[graph_name].append(tmp_val)
                except IndexError:
                    break
        x_label = "Resolution"
        plot_data = {}
        for graph_name in graph_names:
            plot_data.update({graph_name: (res_data, data_dict[graph_name])})
        stat = {"plot_data": plot_data, "plot_axis": x_label}
        return stat

    def get_merging_statistics_data(logfile, graph_names):
        try:
            with open(logfile) as fp:
                json_data = json.load(fp)
        except OSError:
            logger.exception("Error reading data from log file %s", logfile)
            return None
        res_data = json_data["d_star_sq_min"]
        plot_data = {}
        for graph_name, label_name in graph_names:
            try:
                plot_data.update({label_name: (res_data, json_data[graph_name])})
            except KeyError:
                logger.error(
                    "Error reading %s graph data from log file %s",
                    (graph_name, logfile),
                )
        x_label = "Resolution"
        stat = {"plot_data": plot_data, "plot_axis": x_label}
        return stat

    def save_plot(name, rows):
        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()
        ax = [ax1, ax2]
        pt = []
        lb = []
        color = iter(["b", "r", "g", "y", "c", "m"])
        for i, row in enumerate(rows):
            x_axis_title = row["plot_axis"]
            for key, (xdata, ydata) in row["plot_data"].items():
                if xdata and ydata:
                    c = next(color)
                    (p,) = ax[i].plot(xdata, ydata, "o-", c=c, label=key)
                    pt.append(p)
                    lb.append(key)
                    ax[i].set_xlim([xdata[0], xdata[-1]])
                    ax[i].set_ylabel(key)
        ax1.set_xticklabels(
            ["{:.2f}".format(np.float64(1.0) / math.sqrt(x)) for x in ax1.get_xticks()]
        )
        ax1.set_xlabel(x_axis_title)

        plt.legend(pt, lb, loc="upper center", bbox_to_anchor=(0.5, 1.2), ncol=5)
        temp = tempfile.NamedTemporaryFile(prefix="stat_", suffix=".png")
        plt.savefig(temp.name)
        plt.close()
        try:
            with open(temp.name, "rb") as f:
                img_data = f.read()
                tmpl_data["html_images"][name] = img_data
        except OSError:
            pass

    cc_data = None
    for log_file in tmpl_data["xia2_logs"]:
        if "aimless.log" in log_file:
            cc_data = get_plot_data(
                log_file, "Correlations CC(1/2) within dataset,", "CC"
            )
        elif "truncate.log" in log_file:
            anom_data = get_plot_data(
                log_file, "Intensity anomalous analysis", "Mn(dI/sigdI) v resolution"
            )
            meas_data = get_plot_data(
                log_file, "Intensity anomalous analysis", "Mesurability v resolution"
            )
        elif "xia2.html" in log_file:
            xia2_summary_log = os.path.join(
                os.path.dirname(log_file), "xia2-summary.dat"
            )
            with open(xia2_summary_log) as fp:
                tmpl_data["xia2_summary"] = fp.read()
        elif ".stats" in log_file:
            cc_data = get_autoPROC_stats_data(log_file, ["CC(1/2)", "CC(ano)"])
            anom_data = get_autoPROC_stats_data(log_file, ["SigAno"])
    if not cc_data:
        for log_file in tmpl_data["xia2_logs"]:
            if "merging-statistics.json" in log_file and not cc_data:
                cc_data = get_merging_statistics_data(
                    log_file, [("cc_one_half", "CC(1/2)"), ("cc_anom", "CC(ano)")]
                )

    save_plot("graph_cc", [cc_data])
    try:
        save_plot("graph_anom", [anom_data, meas_data])
    except UnboundLocalError:
        save_plot("graph_anom", [anom_data])


def generate_model_snapshots(working_directory, pipeline, tmpl_env):

    logger.info(f"Model path: {working_directory}")
    try:
        mdl_data = get_map_model_from_json(working_directory)
    except Exception:
        logger.info(f"Cannot read map/model data from {working_directory}")
        return
    try:
        map_file_coot = mdl_data["map"]
    except Exception:
        map_file_coot = False
    try:
        pdb_file_coot = mdl_data["pdb"]
    except Exception:
        pdb_file_coot = False
    model_py = os.path.join(working_directory, pipeline + "_models.py")
    coot_sh = os.path.join(working_directory, pipeline + "_models.sh")
    img_name = f"{pipeline}_model"
    coot_py_template = tmpl_env.get_template("coot_model.tmpl")
    with open(model_py, "wt") as f:
        coot_script = coot_py_template.render(
            {
                "map_file": map_file_coot,
                "pdb_file": pdb_file_coot,
                "tag_name": pipeline,
            }
        )
        f.write(coot_script)
    sh_script = [
        "#!/bin/bash",
        f"sh $CCP4/coot_py2/bin/coot --script {model_py} --no-graphics --no-guano --no-state-script --no-startup-scripts",
    ]
    for idx in range(3):
        sh_script.append(
            f"cat raster_{img_name}_{idx}.r3d | render -transparent -png {img_name}_{idx}.png"
        )
    with open(coot_sh, "wt") as f:
        f.write(os.linesep.join(sh_script))
    subprocess.run(["sh", coot_sh], cwd=working_directory)


def read_model_snapshots(working_directory, pipeline, tmpl_data):
    try:
        mdl_data = get_map_model_from_json(working_directory)
        tmpl_data["model_data"][pipeline] = mdl_data["data"]
    except Exception:
        logger.info(f"Cannot read map/model data from {working_directory}")
        return
    img_name = f"{pipeline}_model"
    for idx in range(3):
        img_filename = f"{img_name}_{idx}"
        try:
            with open(
                os.path.join(working_directory, f"{img_filename}.png"), "rb"
            ) as f:
                img_data = f.read()
                tmpl_data["html_images"][img_filename] = img_data
        except OSError:
            pass


def get_pia_plot(tmpl_data, image_number, resolution, spot_count, bragg_candidates):
    fig, ax1 = plt.subplots()
    ax1.set_xlabel("Image number")
    ax1.set_ylabel("Number of spots")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Resolution")

    plt_spots = ax1.scatter(
        image_number, spot_count, c="r", s=75, alpha=0.7, label="Found spots"
    )
    plt_good = ax1.scatter(
        image_number,
        bragg_candidates,
        c="g",
        s=75,
        alpha=0.7,
        label="Good Bragg candidates",
    )
    plt_resol = ax2.scatter(
        image_number, resolution, c="b", s=75, alpha=0.7, label="Resolution"
    )

    ax1.set_xlim([min(image_number), max(image_number)])
    ax2.invert_yaxis()

    ax1.legend(
        (plt_spots, plt_good, plt_resol),
        ("Found spots", "Good Bragg candidates", "Resolution"),
        loc="upper center",
        bbox_to_anchor=(0.5, 1.2),
        ncol=3,
        frameon=False,
    )

    temp = tempfile.NamedTemporaryFile(prefix="pia_", suffix=".png")
    plt.savefig(temp.name)
    plt.close()
    try:
        with open(temp.name, "rb") as f:
            img_data = f.read()
            tmpl_data["html_images"]["img_distl"] = img_data
    except OSError:
        pass


def get_image_files(tmpl_data):
    def read_mime_image(img_path, name):
        try:
            with open(img_path, "rb") as f:
                img_data = f.read()
                tmpl_data["html_images"][name] = img_data
        except OSError:
            logger.info("Cannot read image file %s", img_path)

    jpeg_dir_list = tmpl_data["image_directory"].split(os.sep)
    jpeg_dir_list.insert(6, "jpegs")
    jpeg_dir = os.sep.join(jpeg_dir_list)

    img_prefix = os.path.splitext(tmpl_data["image_template"])[0].replace("#", "")
    img_pattern = "".join([jpeg_dir, os.sep, img_prefix, "*.*t.png"])
    diff_pattern = "".join([jpeg_dir, os.sep, img_prefix, "*.thumb.jpeg"])

    try:
        cryst_img_path = next(iter(sorted(glob.glob(img_pattern))))
        read_mime_image(cryst_img_path, "img_crystal")
    except StopIteration:
        logger.info("Crystal image matching %s pattern not found", img_pattern)
    try:
        diff_img_path = next(iter(sorted(glob.glob(diff_pattern))))
        read_mime_image(diff_img_path, "img_diff")
    except StopIteration:
        logger.info("Diffraction image matching %s pattern not found", diff_pattern)


def get_email_subject(log_file, visit):

    rel_pth = os.path.dirname(log_file).split(os.sep)
    idx_pp = next(i for i, v in enumerate(rel_pth) if "xia2" in v or "autoPROC" in v)
    dataset_relpth = os.sep.join(rel_pth[idx_pp - 2 : idx_pp + 2])
    sub = "->".join([visit, dataset_relpth])
    return sub


def send_html_email_message(msg, pipeline, to_addrs, tmpl_data):
    def add_images(m):

        for cid, img in tmpl_data["html_images"].items():
            try:
                mime_image = MIMEImage(img)
                mime_image.add_header("Content-ID", cid)
                m.attach(mime_image)
            except Exception:
                continue

    try:
        from_addr = "@".join([getpass.getuser(), platform.node()])
        subject = get_email_subject(
            next(iter(tmpl_data["xia2_logs"])), tmpl_data["visit"]
        )

        message = MIMEMultipart("related")
        message["From"] = from_addr
        message["To"] = ",".join(to_addrs)
        message["Subject"] = " ".join([f"[phasing-html:{pipeline}]", subject])
        txt = MIMEText(msg, "html")
        message.attach(txt)
        add_images(message)

        server = smtplib.SMTP("localhost")
        retry = 5
        for i in range(retry):
            try:
                server.sendmail(
                    from_addr=from_addr, to_addrs=to_addrs, msg=message.as_string()
                )
                logger.info("Sent email with big_ep results")
                return
            except SMTPSenderRefused:
                sleep(60)
        logger.error("Cannot sending email with big_ep processing results")
    except Exception:
        logger.exception("Error sending email with big_ep processing results")

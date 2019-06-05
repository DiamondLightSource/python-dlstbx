from __future__ import absolute_import, division, print_function

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import glob
from iotbx import data_plots
import json
import logging
import math
import numpy as np
import os
import smtplib, platform, getpass
import subprocess
import tempfile

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt

from smtplib import SMTPSenderRefused
from time import sleep


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
                    for (plot, y_label) in zip(plots, graph.y_labels):
                        plot_data.update({y_label: plot})
                    stat = {"plot_data": plot_data, "plot_axis": x_label}
                    return stat
            except Exception:
                logger.exception(
                    "Error reading table %s from %s", (table_name, logfile)
                )
                continue

    def save_plot(name, rows):
        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()
        ax = [ax1, ax2]
        pt = []
        lb = []
        color = iter(["b", "r", "g", "y", "c", "m"])
        for i, row in enumerate(rows):
            x_axis_title = row["plot_axis"]
            for key, (xdata, ydata) in row["plot_data"].iteritems():
                if xdata and ydata:
                    c = next(color)
                    p, = ax[i].plot(xdata, ydata, "o-", c=c, label=key)
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
        except IOError:
            pass

    for log_file in tmpl_data["xia2_logs"]:
        if "aimless.log" in log_file:
            xia2_aimless_log = log_file
        if "truncate.log" in log_file:
            xia2_sad_log = log_file
        if "xia2.html" in log_file:
            xia2_summary_log = os.path.join(
                os.path.dirname(log_file), "xia2-summary.dat"
            )
            with open(xia2_summary_log) as fp:
                tmpl_data["xia2_summary"] = fp.read()
    cc_data = get_plot_data(
        xia2_aimless_log, "Correlations CC(1/2) within dataset,", "CC"
    )
    anom_data = get_plot_data(
        xia2_sad_log, "Intensity anomalous analysis", "Mn(dI/sigdI) v resolution"
    )
    meas_data = get_plot_data(
        xia2_sad_log, "Intensity anomalous analysis", "Mesurability v resolution"
    )

    save_plot("graph_cc", [cc_data])
    if "xia2" in xia2_sad_log:
        save_plot("graph_anom", [anom_data, meas_data])
    elif "autoPROC" in xia2_sad_log:
        with open(
            os.path.join(
                os.path.dirname(xia2_aimless_log), "aimless.mrfana.SigAno.png"
            ),
            "rb",
        ) as f:
            img_data = f.read()
            tmpl_data["html_images"]["graph_anom"] = img_data


def read_settings_file(tmpl_data):

    json_path = next(
        iter(
            glob.glob(
                os.path.join(tmpl_data["big_ep_path"], "*", "*", "big_ep_settings.json")
            )
        )
    )
    with open(json_path, "r") as json_file:
        msg_json = json.load(json_file)
        tmpl_data.update({"settings": msg_json})


def get_map_model_from_json(json_path):

    abs_json_path = os.path.join(json_path, "big_ep_model_ispyb.json")
    with open(abs_json_path, "r") as json_file:
        msg_json = json.load(json_file)
    return {
        "pdb": msg_json["pdb"],
        "map": msg_json["map"],
        "data": {
            "residues": "{0}".format(msg_json["total"]),
            "max_frag": "{0}".format(msg_json["max"]),
            "frag": "{0}".format(msg_json["fragments"]),
            "mapcc": "{0:.2f} ({1:.2f})".format(
                msg_json["mapcc"], msg_json["mapcc_dmin"]
            ),
        },
    }


def generate_model_snapshots(tmpl_env, tmpl_data):
    root_wd = tmpl_data["big_ep_path"]
    paths = [
        p for p in glob.glob(os.path.join(root_wd, "*", "*", "*")) if os.path.isdir(p)
    ]

    try:
        autosharp_path = next(iter(filter(lambda p: "autoSHARP" in p, paths)))
    except StopIteration:
        autosharp_path = None
    try:
        autosol_path = next(iter(filter(lambda p: "AutoSol" in p, paths)))
    except StopIteration:
        autosol_path = None
    try:
        crank2_path = next(iter(filter(lambda p: "crank2" in p, paths)))
    except StopIteration:
        crank2_path = None

    tmpl_data.update({"model_images": {}})
    tmpl_data.update({"model_data": {}})

    for tag_name, map_model_path in {
        "autoSHARP": autosharp_path,
        "AutoBuild": autosol_path,
        "Crank2": crank2_path,
    }.iteritems():
        try:
            mdl_data = get_map_model_from_json(map_model_path)
        except Exception:
            logger.info("Cannot read map/model data from %s", map_model_path)
            continue

        try:
            map_file_coot = mdl_data["map"]
        except Exception:
            map_file_coot = False

        try:
            pdb_file_coot = mdl_data["pdb"]
        except Exception:
            pdb_file_coot = False

        model_py = os.path.join(root_wd, tag_name + "_models.py")
        coot_sh = os.path.join(root_wd, tag_name + "_models.sh")

        img_name = "{0}_model".format(tag_name)

        coot_py_template = tmpl_env.get_template("coot_model.tmpl")
        with open(model_py, "wt") as f:
            coot_script = coot_py_template.render(
                {
                    "map_file": map_file_coot,
                    "pdb_file": pdb_file_coot,
                    "tag_name": tag_name,
                }
            )
            f.write(coot_script)

        with open(os.path.join(root_wd, coot_sh), "wt") as f:
            f.write(
                os.linesep.join(
                    [
                        "#!/bin/bash",
                        ". /etc/profile.d/modules.sh",
                        "module purge",
                        "module load ccp4",
                        "module load python/ana",
                        "coot --python {0} --no-graphics --no-guano".format(model_py),
                        "cat raster_{0}.r3d | render -transparent -png {0}.png".format(
                            img_name
                        ),
                    ]
                )
            )

        subprocess.call(["sh", coot_sh])

        try:
            with open("{0}.png".format(img_name), "rb") as f:
                img_data = f.read()
                tmpl_data["html_images"][img_name] = img_data
        except IOError:
            pass
        tmpl_data["model_data"].update({tag_name: mdl_data["data"]})


def get_pia_plot(tmpl_data, pia_results):

    img, resol, spots, good_spots = zip(*pia_results)

    fig, ax1 = plt.subplots()
    ax1.set_xlabel("Image number")
    ax1.set_ylabel("Number of spots")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Resolution")

    plt_spots = ax1.scatter(img, spots, c="r", s=75, alpha=0.7, label="Found spots")
    plt_good = ax1.scatter(
        img, good_spots, c="g", s=75, alpha=0.7, label="Good Bragg candidates"
    )
    plt_resol = ax2.scatter(img, resol, c="b", s=75, alpha=0.7, label="Resolution")

    ax1.set_xlim([min(img), max(img)])
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
    except IOError:
        pass


def get_image_files(tmpl_data):
    def read_mime_image(img_path, name):
        try:
            with open(img_path, "rb") as f:
                img_data = f.read()
                tmpl_data["html_images"][name] = img_data
        except IOError:
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
    dataset_relpth = os.sep.join(
        rel_pth[idx_pp - 2 : idx_pp + 2] + [os.path.basename(log_file)]
    )
    sub = "->".join([visit, dataset_relpth])
    return sub


def send_html_email_message(msg, to_addrs, tmpl_data):
    def add_images(m):

        for cid, img in tmpl_data["html_images"].iteritems():
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
        message["Subject"] = " ".join(["[phasing-html]", subject])
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

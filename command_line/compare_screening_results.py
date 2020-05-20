import libtbx.phil

phil_scope = libtbx.phil.parse(
    """\
"""
)


def run(args):
    from dlstbx.ispybtbx import ispybtbx

    interp = phil_scope.command_line_argument_interpreter()
    params, unhandled = interp.process_and_fetch(
        args, custom_processor="collect_remaining"
    )
    params = params.extract()

    args = unhandled

    assert len(args) > 0
    visit_name = args[0]
    ispyb_conn = ispybtbx()
    sessionid = ispyb_conn.get_bl_sessionid_from_visit_name(visit_name)
    print(sessionid)

    sql_str = (
        """
select datacollectionid
from DataCollection
where DataCollection.sessionid = %s
;
"""
        % sessionid
    )

    results = ispyb_conn.execute(sql_str)
    from libtbx.utils import flat_list

    dc_ids = flat_list(results)
    print(dc_ids)

    columns = [
        "Screening.dataCollectionId",
        "Screening.programVersion",
        "Screening.comments",
        "Screening.shortComments",
        "ScreeningOutput.mosaicity",
        "ScreeningStrategyWedge.resolution",
        "ScreeningStrategyWedge.numberOfImages",
        "ScreeningStrategySubWedge.transmission",
        "ScreeningStrategySubWedge.exposureTime",
        "ScreeningOutputLattice.spaceGroup",
        "ScreeningOutputLattice.unitCell_a",
        "ScreeningOutputLattice.unitCell_b",
        "ScreeningOutputLattice.unitCell_c",
        "ScreeningOutputLattice.unitCell_alpha",
        "ScreeningOutputLattice.unitCell_beta",
        "ScreeningOutputLattice.unitCell_gamma",
    ]

    # columns = None
    field_names, rows = ispyb_conn.get_screening_results(dc_ids, columns=columns)
    rows = [[str(i) for i in r] for r in rows]
    rows.insert(0, field_names)

    from libtbx import table_utils

    print(table_utils.format(rows=rows, has_header=True))

    from scitbx.array_family import flex

    d = {}

    plots = [
        "transmission",
        "mosaicity",
        "resolution",
        "numberOfImages",
        "spaceGroup",
        "exposureTime",
    ]
    unit_cell = [
        "unitCell_a",
        "unitCell_b",
        "unitCell_c",
        "unitCell_alpha",
        "unitCell_beta",
        "unitCell_gamma",
    ]
    for k in (
        ["dataCollectionId", "programVersion", "comments", "shortComments"]
        + plots
        + unit_cell
    ):
        idx = field_names.index(k)
        values = flex.std_string(str(row[idx]) for row in rows[1:])
        d[k] = values

    dc_ids = d["dataCollectionId"]
    unique_dc_ids = set(dc_ids)

    program_version = d["programVersion"]
    short_comments = d["shortComments"]

    selections = {"xia2": {}, "edna": {}}

    selections["xia2"]["native"] = (program_version == "xia2.strategy") & (
        short_comments == "native"
    )
    selections["edna"]["native"] = (program_version == "EDNA MXv1") & (
        short_comments == "EDNAStrategy1"
    )
    selections["xia2"]["anomalous"] = (program_version == "xia2.strategy") & (
        short_comments == "anomalous"
    )
    selections["edna"]["anomalous"] = (program_version == "EDNA MXv1") & (
        short_comments == "EDNAStrategy2"
    )
    selections["xia2"]["high multiplicity"] = (program_version == "xia2.strategy") & (
        short_comments == "high multiplicity"
    )
    selections["edna"]["high multiplicity"] = (program_version == "EDNA MXv1") & (
        short_comments == "EDNAStrategy3"
    )
    selections["xia2"]["gentle"] = (program_version == "xia2.strategy") & (
        short_comments == "gentle"
    )
    selections["edna"]["gentle"] = (program_version == "EDNA MXv1") & (
        short_comments == "EDNAStrategy4"
    )

    from cctbx import sgtbx

    d["spaceGroup"] = flex.std_string(
        [
            str(sgtbx.space_group_info(sg).type().number()) if sg != "None" else sg
            for sg in d["spaceGroup"]
        ]
    )

    from matplotlib import pyplot

    for p in plots:
        for strategy in ("native", "anomalous", "high multiplicity", "gentle"):
            x = []
            y = []
            for dc_id in unique_dc_ids:
                sel = dc_ids == dc_id
                xia2_isel = (sel & selections["xia2"][strategy]).iselection()
                edna_isel = (sel & selections["edna"][strategy]).iselection()

                if xia2_isel.size() and edna_isel.size():
                    x.append(d[p].select(xia2_isel)[0])
                    y.append(d[p].select(edna_isel)[0])

            pyplot.scatter(x, y, label=strategy, marker="+")

        pyplot.xlabel("xia2.strategy")
        pyplot.ylabel("EDNA")
        pyplot.legend()
        pyplot.title(p)
        lim = max(pyplot.xlim()[1], pyplot.ylim()[1])
        pyplot.xlim(0, lim)
        pyplot.ylim(0, lim)
        pyplot.axes().set_aspect("equal")
        pyplot.plot([0, lim], [0, lim], c="black", zorder=0)
        pyplot.savefig("%s.png" % p)
        pyplot.clf()

    for uc in "abc":
        x = []
        y = []

        fig = pyplot.figure()
        from matplotlib.gridspec import GridSpec

        gs = GridSpec(4, 4)
        ax_scatter = fig.add_subplot(gs[1:4, 0:3])
        ax_hist_x = fig.add_subplot(gs[0, 0:3])
        ax_hist_y = fig.add_subplot(gs[1:4, 3])

        for dc_id in unique_dc_ids:
            sel = dc_ids == dc_id
            xia2_isel = (sel & selections["xia2"]["native"]).iselection()
            edna_isel = (sel & selections["edna"]["native"]).iselection()

            if xia2_isel.size() and edna_isel.size():
                x.append(float(d["unitCell_%s" % uc].select(xia2_isel)[0]))
                y.append(float(d["unitCell_%s" % uc].select(edna_isel)[0]))

        ax_scatter.scatter(x, y, label=uc, marker="+")

        ax_hist_x.hist(x)
        ax_hist_y.hist(y, orientation="horizontal")
        pyplot.title("unit_cell_%s" % uc)
        lim = (
            min(ax_scatter.get_xlim()[0], ax_scatter.get_ylim()[0]),
            max(ax_scatter.get_xlim()[1], ax_scatter.get_ylim()[1]),
        )
        ax_scatter.set_xlim(lim)
        ax_scatter.set_ylim(lim)
        ax_hist_x.set_xlim(lim)
        ax_hist_y.set_ylim(lim)
        ax_scatter.set_xlabel("xia2.strategy")
        ax_scatter.set_ylabel("EDNA")
        pyplot.setp(ax_hist_x.get_xticklabels(), visible=False)
        pyplot.setp(ax_hist_y.get_yticklabels(), visible=False)
        ax_scatter.plot(lim, lim, c="black", zorder=0)
        pyplot.savefig("unit_cell_%s.png" % uc)
        pyplot.clf()


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    run(args)

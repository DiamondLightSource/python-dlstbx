import sys
import libtbx.phil

phil_scope = libtbx.phil.parse(
    """\
statistics_type = outerShell innerShell *overall
  .type = choice
"""
)


def run(args=None):
    if not args:
        args = sys.argv[1:]

    from dlstbx.ispybtbx import ispybtbx

    interp = phil_scope.command_line_argument_interpreter()
    params, unhandled = interp.process_and_fetch(
        args, custom_processor="collect_remaining"
    )
    params = params.extract()

    args = unhandled

    assert len(args) > 0
    dc_ids = []
    columns = []
    for arg in args:
        try:
            dc_ids.append(int(arg))
        except ValueError:
            columns.append(arg)

    if len(columns) == 0:
        columns = None
    elif len(dc_ids) > 1:
        columns.insert(0, "AutoProcIntegration.dataCollectionId")

    ispyb_conn = ispybtbx()
    field_names, rows = ispyb_conn.get_processing_statistics(
        dc_ids, columns=columns, statistics_type=params.statistics_type
    )
    rows = [[str(i) for i in r] for r in rows]
    rows.insert(0, field_names)

    from libtbx import table_utils

    print(table_utils.format(rows=rows, has_header=True))


if __name__ == "__main__":
    run()

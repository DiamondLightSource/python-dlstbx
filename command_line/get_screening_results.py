from __future__ import absolute_import, division, print_function


def run(args):
    from dlstbx.ispybtbx import ispybtbx

    ispyb_conn = ispybtbx()

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
        columns.insert(0, "Screening.dataCollectionID")

    field_names, rows = ispyb_conn.get_screening_results(dc_ids, columns=columns)
    rows = [[str(i) for i in r] for r in rows]
    rows.insert(0, field_names)

    from libtbx import table_utils

    print(table_utils.format(rows=rows, has_header=True))


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    run(args)

import ispyb
import ispyb.model.__future__
import json
import sys

import libtbx.phil

ispyb.model.__future__.enable("/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg")

phil_scope = libtbx.phil.parse(
    """\
json = None
  .type = path
show = True
  .type = bool
"""
)


def run(args):

    interp = phil_scope.command_line_argument_interpreter()
    params, args = interp.process_and_fetch(args, custom_processor="collect_remaining")
    params = params.extract()

    assert len(args) > 0
    dcids = [int(arg) for arg in args]
    assert len(dcids) > 0

    with ispyb.open(
        "/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg"
    ) as ispyb_conn:

        results = {}
        for dcid in dcids:
            dc = ispyb_conn.get_data_collection(dcid)
            results[dcid] = {}
            for d in dc.image_quality._data:
                for k, v in d.items():
                    results[dcid].setdefault(k, [])
                    results[dcid][k].append(v)
            if params.show:
                print(dc.image_quality)

        if params.json:
            with open(params.json, "wb") as fh:
                json.dump(results, fh, indent=2)


if __name__ == "__main__":
    run(sys.argv[1:])

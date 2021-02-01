import sys
import ispyb.model.datacollection
import ispyb.model.screening
import ispyb.model.__future__

ispyb.model.__future__.enable("/dls_sw/apps/zocalo/secrets/credentials-ispyb.cfg")


def run(args=None):
    if not args:
        args = sys.argv[1:]
    with ispyb.open(
        "/dls_sw/apps/zocalo/secrets/credentials-ispyb-sp.cfg"
    ) as ispyb_conn:
        dcids = [int(arg) for arg in args]

        for dcid in dcids:
            dc = ispyb_conn.get_data_collection(dcid)
            for screening in dc.screenings:
                print(screening)
                for screening_output in screening.outputs:
                    print(screening_output)
                    for lattice in screening_output.lattices:
                        print(lattice)
                    for strategy in screening_output.strategies:
                        print(strategy)
                        for wedge in strategy.wedges:
                            print(wedge)
                            for sub_wedge in wedge.sub_wedges:
                                print(sub_wedge)
                print()


if __name__ == "__main__":
    run()

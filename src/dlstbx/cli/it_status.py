#!/usr/bin/env python
#
# LIBTBX_SET_DISPATCHER_NAME it.status

import logging
import operator
import sys
from optparse import SUPPRESS_HELP, OptionGroup, OptionParser

import junit_xml

import dlstbx.health_checks
from dlstbx.util.colorstreamhandler import ColorStreamHandler


def run():
    parser = OptionParser(usage="it.status [options] [issue | issuegroup.]*")
    parser.add_option("-?", action="help", help=SUPPRESS_HELP)
    parser.add_option(
        "-v",
        action="count",
        dest="verbosity",
        default=0,
        help="Increase verbosity level (up to 3x)",
    )
    parser.add_option(
        "-q", action="count", dest="quiet", default=0, help="Be more quiet (up to 2x)"
    )
    parser.add_option(
        "-o",
        "--output",
        "--xml",
        dest="output",
        metavar="OUTFILE",
        default=None,
        help="Write status to a JUnit XML file",
    )
    parser.add_option(
        "--generate-templates",
        dest="templates",
        action="store_true",
        default=False,
        help="write Jenkins notification templates to current directory",
    )

    report = OptionGroup(parser, "to add a report to the database")
    report.add_option(
        "-s",
        "--source",
        dest="source",
        metavar="SRC",
        default=None,
        help="Add a report for this source to the database",
    )
    report.add_option(
        "-l",
        "--level",
        dest="level",
        metavar="LVL",
        type=int,
        default=0,
        help="Warning level (0-9: OK, 11-19: Warn, 20+: Error)",
    )
    report.add_option(
        "-m",
        "--message",
        dest="message",
        metavar="MSG",
        default=None,
        help="A tweet-long (<140 chars) status message",
    )
    report.add_option(
        "-u",
        "--url",
        dest="URL",
        default=None,
        help="Optional link to more information",
    )
    parser.add_option_group(report)

    prune = OptionGroup(parser, "to clean up the database")
    prune.add_option(
        "--prune",
        dest="prune",
        action="store_true",
        default=False,
        help="Remove reports older than 24 hours",
    )
    parser.add_option_group(prune)

    (options, args) = parser.parse_args()

    db = dlstbx.health_checks.database()

    if options.prune:
        records = db.prune()
        print(f"Database successfully pruned, {records} entries removed")

    if options.source:
        db.set_status_components(
            source=options.source,
            level=options.level,
            message=options.message,
            url=options.URL,
        )
        if not args and not options.output:
            exit()

    status = sorted(db.get_status(), key=operator.attrgetter("Level"), reverse=True)
    if args:
        prefixes = tuple(x.rstrip(".") + "." for x in args)
        status = [
            s for s in status if s.Source in args or s.Source.startswith(prefixes)
        ]

    if options.templates:
        generate_templates(status)

    if options.output:
        if not status:
            exit("No records found in database")
        most_recent_test = sorted(s.Timestamp for s in status)[-1]
        test_suite = junit_xml.TestSuite(
            "it.status",
            timestamp=most_recent_test.isoformat(),
            test_cases=[s.as_testcase() for s in status],
        )

        with open(options.output, "w") as fh:
            junit_xml.to_xml_report_file(
                fh,
                [test_suite],
                prettyprint=True,
                encoding="UTF-8",
            )
        exit()

    if hasattr(ColorStreamHandler, "_get_color"):

        def setbold():
            sys.stdout.write(ColorStreamHandler.BOLD)

        def setcolor(level):
            sys.stdout.write(getattr(ColorStreamHandler, "_get_color")(level))

        def resetcolor():
            sys.stdout.write(ColorStreamHandler.DEFAULT)

    else:
        setbold = lambda: None
        setcolor = lambda x: None
        resetcolor = lambda: None

    error_seen = False
    for group, colour in (
        ("Error", logging.ERROR),
        ("Warning", logging.WARNING),
        ("Information", logging.INFO),
    ):
        select = [s for s in status if s.Group == group]
        if select:
            resetcolor()
            setcolor(colour)
            if not options.quiet:
                if options.verbosity > 0 or group != "Information":
                    setbold()
                    print(
                        "\n%d %s message%s:"
                        % (len(select), group, "" if len(select) == 1 else "s")
                    )
                else:
                    print(
                        "\n%d %s message%s omitted"
                        % (len(select), group, "" if len(select) == 1 else "s")
                    )
            if group == "Information" and (options.quiet or options.verbosity == 0):
                continue
            if group == "Error":
                error_seen = True
            base_indent = "" if options.quiet else "  "
            for s in select:
                resetcolor()
                setcolor(colour)
                if s.Level > 0:
                    setbold()
                print(f"{base_indent}{s.Source}", end=": ")
                resetcolor()
                setcolor(colour)
                print(f"{s.Message} ({s.age_str})")
                indent = base_indent + (len(s.Source) + 2) * " "
                if (
                    s.MessageBody
                    and not options.quiet
                    and (group != "Information" or options.verbosity > 2)
                ):
                    print(indent + s.MessageBody.replace("\n", "\n" + indent))
                if (
                    s.URL
                    and (group != "Information" or options.verbosity > 1)
                    and not (options.quiet > 1)
                ):
                    print(indent + s.URL)
    resetcolor()
    if error_seen:
        exit(1)


def generate_templates(status):
    pass

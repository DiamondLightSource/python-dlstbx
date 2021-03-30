# Commands

## `ispyb.job`

Create or update, or view information for an entry in the ISPyB ProcessingJob table,
optionally triggering the job after creation via a call to [`dlstbx.go`](#dlstbxgo).

### Examples

Create a new processing job:
```bash
ispyb.job --new --display "Dataprocessor 2000" --comment "The best program in the universe" \
          --recipe dp2000 --add-param "spacegroup:P 21 21 21" --add-sweep 1234:1:600
```

Display stored information:
```bash
ispyb.job 73
ispyb.job 73 -v  # show full record
```

Create new processing program row:
```bash
ispyb.job 73 -c -p "program" -s "starting up..."
```

Update stored information:
```bash
ispyb.job 73 -u 1234 -s "running..."
ispyb.job 73 -u 1234 -s "things are happening" --update-time "2017-08-25"
ispyb.job 73 -u 1234 -s "completed successfully" -r success
ispyb.job 73 -u 1234 -s "everything is broken" -r failure
```

## `dlstbx.go`

Trigger processing of a standard recipe, of an arbitrary recipe from a local
file, or of an entry in the ISPyB processing table. The recipe will be sent to
the `processing_recipe` queue.

### Examples

Run recipe for the given DCID:
```bash
dlstbx.go -r example-xia2 527189
```

Run recipe using an existing reprocessing ID:
```bash
dlstbx.go -r example-xia2 -p 12345
```

Run arbitrary recipe from local file, without providing a DCID:
```bash
dlstbx.go -f /path/to/recipe.json -n
```

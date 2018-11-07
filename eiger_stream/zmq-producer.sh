#!/bin/bash

cd /dls_sw/work/tools/RHEL6-x86_64/eiger-detector/testdata/16_large

#/dls_sw/prod/tools/RHEL6-x86_64/eiger-detector/1-2-1/prefix/bin/eigerSim -a grid_1234 -f streamfile -n 4000 -z 560
# This will send 4000 frames at 560Hz with an acquisition ID of grid_1234

/dls_sw/prod/tools/RHEL6-x86_64/eiger-detector/1-2-1/prefix/bin/eigerSim -a grid_1234 -f streamfile -n 4000 -z 10


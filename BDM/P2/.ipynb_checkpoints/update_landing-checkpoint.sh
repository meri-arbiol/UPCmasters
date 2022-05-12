#! /bin/bash

BASEDIR=./

# running opendatabcn collector script for accidents
echo "Checking for accidents updates from opendataBCN..."
python opendata_collector.py accidents

# running opendatabcn collector script for lloguer preu
echo "Checking for lloguer preu updates from opendataBCN..."
python opendata_collector.py lloguer_preu
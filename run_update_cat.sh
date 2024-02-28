#!/bin/bash
#wrapper script so that can enter python venv before running update catalog so can have access to pystac and cogeo
source ./stac-cat-env/bin/activate
python updatecatalog.py


#!/bin/bash
cd /home/dylan/wncat 
/nix/var/nix/profiles/default/bin/nix develop --command python updatecatalog.py 

This repository demonstrates a working pipeline to pull VIIRS 1 day composite floodwater from [this directory](https://floodlight.ssec.wisc.edu/composite/) on the SSEC floodwatch http server. This pipeline assumes that a static catalog is being created and written to the WPN’s public s3 bucket.

I was currently in the middle of reworking this pipeline to integrate with the WPN STAC API deployed in February, 2024. The python script with the in-progress pipeline is called "stac-server-testcatalog.py".

The scripts are designed to use cron to pull data off the ssec floodlight server on a regular basis.

This code needs to be run inside two environments. A nix devshell that is entered with the command “nix develop”. Then activate the python venv called “stac-cat-env”. The two environment setup necessitated wrapping the cron execution in two shell scripts. One called “dailycatrun.sh” runs the script “run_update_cat.sh” inside the nix development environment. run_update_cat.sh activates the python environment and then runs the script “updatecatalog.py”. So “dailycatrun.sh” is the script that is run as a cron job on a daily cadence to pull in the daily updates to the viirs-1-day-composite data.

The catalog needs to be initiated using the script “makecatalog.py” and then cron runs “updatecatalog.py” on the initiated catalog.

“stac_mod.py” has been modified since the working pipeline was functioning so some of the functions being called by “makecatalog.py” and “updatecatalog.py” may currently throw errors unless you roll back to a commit from Fall, 2023.

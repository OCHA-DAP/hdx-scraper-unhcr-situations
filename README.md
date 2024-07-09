### Collector for UNHCR Situation Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-unhcr-situations/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-unhcr-situations/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-unhcr-situations/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-unhcr-situations?branch=main)

This script extracts data and metadata from [UNHCR Situations](https://data.unhcr.org/en/regions) and creates one dataset in HDX. It makes around 35 reads from the UNHCR API and two read/writes (API calls) to HDX in a one hour period. It creates around 35 temporary files of a few Kb, one of which it uploads into HDX. It is run every week.


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-unhcr-situations** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY
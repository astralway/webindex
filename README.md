# WebIndex

[![Build Status](https://travis-ci.org/fluo-io/webindex.svg?branch=master)](https://travis-ci.org/fluo-io/webindex)

An example Fluo applications that creates a web index using CommonCrawl data.

### Requirements

In order run this application you need the following installed and running on your
machine:

* Hadoop (HDFS & YARN)
* Accumulo
* Fluo

Consider using [fluo-dev] to run these requirements

### Configure your environment

First, you must create the configuration files `data.yml` and `dropwizard.yml` files in the `conf/` 
directory and edit them for your environment:

    cp conf/data.yml.example conf/data.yml
    cp conf/dropwizard.yml.example conf/dropwizard.yml

### Copy CommonCrawl data from AWS into HDFS

CommonCrawl data sets are hosted in S3.  The following command will run a Spark job that copies
data files from AWS into HDFS.  This script is configured by `conf/data.yml` where you can specify
the HDFS data directory, number of files to copy, and number of executors to run.

    ./bin/copy.sh

### Initialize Fluo & Accumulo with data

After you have copied data to HDFS, run the following command to run a Spark job that will index
your data in HDFS and initialize Accumulo and Fluo with data.

    ./bin/init.sh

### Run the web application

Finally, run the following command to run the web app:

    ./bin/webapp.sh

Open your browser to [http://localhost:8080/](http://localhost:8080/)

[fluo-dev]: https://github.com/fluo-io/fluo-dev

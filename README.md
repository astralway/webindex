# WebIndex

[![Build Status](https://travis-ci.org/fluo-io/webindex.svg?branch=master)](https://travis-ci.org/fluo-io/webindex)

An example Fluo applications that indexes links to web pages in multiple ways.
The example uses CommonCrawl data as input.  See the
[tables](docs/tables.md) and [code](docs/code-guide.md) documentation for
more information about how this example works.

### Requirements

In order run this application you need the following installed and running on your
machine:

* Hadoop (HDFS & YARN)
* Accumulo
* Fluo

Consider using [fluo-dev] to run these requirements

### Configure your environment

First, you must create the configuration file `data.yml` in the `conf/` directory and edit it
for your environment:

    cp conf/data.yml.example conf/data.yml

There are a few environment variables that need to be set to run these scripts (see `conf/webindex-env.sh.example`
for a list).  If you don't want to set them in your `~/.bashrc`, create `webindex-env.sh` in `conf/` and set them.

    cp conf/webindex-env.sh.example conf/webindex-env.sh

### Copy CommonCrawl data from AWS into HDFS

CommonCrawl data sets are hosted in S3.  The following command will run a Spark job that copies
data files from AWS into HDFS `init/` and `load/` directoires.  This script is configured by 
`conf/data.yml` where you can specify the number of files to copy into each directory:

    ./bin/webindex copy

### Initialize Fluo & Accumulo with data

After you have copied data into HDFS, run the following command to run a Spark job that will initialize 
Fluo and Accumulo with data in your HDFS `init/` directory.

    ./bin/webindex init

### Load files into Fluo

The `init` command can only be run on an empty cluster.  To add more data, run the `load` which starts
a Spark job that loads Fluo with data in your HDFS `load/` directory.  Fluo will incrementally process 
this data and update Accumulo.

    ./bin/webindex copy

### Run the webindex UI

Run the following command to run the webindex UI:

    ./bin/webindex ui

The UI is implemented useing [dropwizard].  While the default dropwizard configuration works well, you can
modify it by creating and editing `dropwizard.yml` in `conf/`:
    
    cp conf/dropwizard.yml.example conf/dropwizard.yml

Open your browser to [http://localhost:8080/](http://localhost:8080/)

[fluo-dev]: https://github.com/fluo-io/fluo-dev
[dropwizard]: http://dropwizard.io/

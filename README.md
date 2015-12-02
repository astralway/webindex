# WebIndex

[![Build Status](https://travis-ci.org/fluo-io/webindex.svg?branch=master)](https://travis-ci.org/fluo-io/webindex)

Webindex is an example Fluo application that uses [CommonCrawl][cc] web crawl data to index 
links to web pages in multiple ways.  It has a simple UI to view the resulting indexes.  If 
you are new to Fluo, you may want start with the [fluo-quickstart][qs] or [phrasecount][pc] 
applications as the webindex application is more complicated.  For more information on the
webindex application works, check out the [tables](docs/tables.md) and 
[code](docs/code-guide.md) documentation.

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

There are a few environment variables that need to be set to run these scripts (see 
`conf/webindex-env.sh.example` for a list).  If you don't want to set them in your `~/.bashrc`, 
create `webindex-env.sh` in `conf/` and set them.

    cp conf/webindex-env.sh.example conf/webindex-env.sh

### Download the paths file for a crawl

For each crawl of the web, CommonCrawl produces a file containing a list of paths to the data 
files produced by that crawl.  Webindex uses this file to retrieve CommonCrawl data stored 
in S3. The command below downloads this paths file for the April 2015 crawl (identified by 
`2015-18`) to the `paths/` directory as it will be necessary for future commands.  If you 
would like to use a different crawl, the [CommonCrawl website][cdata] has a list of possible 
crawls which are identified by the `YEAR-WEEK` (i.e. `2015-18`) of the time the crawl 
occurred.

    ./bin/webindex getpaths 2015-18

Take a look at the paths file that was just retrieved:

    $ less paths/2015-18.wat.paths 

Each line in the paths file contains a path to a different common crawl data file.  In later 
commands, you will select paths by specifying a range (in the format of `START-END`).  Ranges
can start at index 0 and their start/end points are inclusive.  Therefore, a range of `4-6` 
would select 3 paths from line 4, 5, and 6 of the file. Using the command below, you can 
find the max endpoint for ranges in a paths file.

    $ wc -l paths/2015-18.wat.paths 
    38609 paths/2015-18.wat.paths

The 2015-18 paths file has 38609 different paths.  A range of `0-38608` would select all 
paths in the file.

### Copy CommonCrawl data from AWS into HDFS

After retrieving a paths file, the command below runs a Spark job that copies data files from S3 
to HDFS.  The command below will copy 3 files in the file range of `4-6` of the `2015-18` paths 
file into the HDFS directory `/cc/data/a`.  CommonCrawl data files are large (~330 MB each) so
be mindful of how many you copy.

    ./bin/webindex copy 2015-18 4-6 /cc/data/a

To create multiple data sets, run the command with different range and HDFS directory:

    ./bin/webindex copy 2015-18 7-8 /cc/data/b

### Initialize Fluo & Accumulo

After copying data into HDFS, run the following to initialize and start Fluo.

    ./bin/webindex init

Optionally, add a HDFS directory (with previously copied data) to the end of the command.  
When a directory is specified, `init` will also run a Spark job that initializes Fluo's 
table in Accumulo with data before starting Fluo.
    
    ./bin/webindex init /cc/data/a

### Load files into Fluo

The `init` command should only be run on an empty cluster.  To add more data, run the 
`load-hdfs` or `load-s3` commands.  Both start a Spark job that parses CommonCrawl and 
inserts this data into Fluo.  Fluo will incrementally process this data and export indexes 
to Accumulo.

The `load-hdfs` command below loads data stored in the HDFS directory `/cc/data/b` into 
Fluo:

    ./bin/webindex load-hdfs /cc/data/b

The `load-s3` command below loads data hosted on S3 into Fluo.  It select files in the 
`9-10` range of the `2015-18` paths file:

    ./bin/webindex load-s3 2015-18 9-10

### Run the webindex UI

Run the following command to run the webindex UI:

    ./bin/webindex ui

The UI is implemented using [dropwizard].  While the UI works with default dropwizard 
configuration, you can modify it by creating and editing `dropwizard.yml` in `conf/`:
    
    cp conf/dropwizard.yml.example conf/dropwizard.yml

Open your browser to [http://localhost:8080/](http://localhost:8080/)

[qs]: https://github.com/fluo-io/fluo-quickstart
[pc]: https://github.com/fluo-io/phrasecount
[fluo-dev]: https://github.com/fluo-io/fluo-dev
[dropwizard]: http://dropwizard.io/
[cc]: https://commoncrawl.org/
[cdata]: https://commoncrawl.org/the-data/get-started/

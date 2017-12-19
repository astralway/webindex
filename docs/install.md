# WebIndex Install

Below are instructions for installing WebIndex on a cluster.

## Requirements

To run WebIndex, you need the following installed on your cluster:

* Java
* Hadoop (HDFS & YARN)
* Accumulo
* Fluo
* Maven

Hadoop & Accumulo should be running before starting these instructions.  Fluo and Maven only need to
be installed on the machine where you run the `webindex` command. Consider using [Uno] to setup 
Hadoop, Accumulo & Fluo if you are running on a single node.

## Configure your environment

First, clone the WebIndex repo:

    git clone https://github.com/astralway/webindex.git

Copy the configuration files in `conf/examples` to `conf/` and edit for your environment:

    cd webindex/conf/
    cp examples/* .
    vim webindex.yml
    vim webindex-env.sh

## Download the paths file for a crawl

For each crawl of the web, Common Crawl produces a file containing a list of paths to the data 
files produced by that crawl.  The webindex `copy` and `load-s3` commands use this file to 
retrieve Common Crawl data stored in S3. The `getpaths` command below downloads this paths 
file for the April 2015 crawl (identified by `2015-18`) to the `paths/` directory as it will 
be necessary for future commands.  If you would like to use a different crawl, the 
[Common Crawl website][cdata] has a list of possible crawls which are identified by the 
`YEAR-WEEK` (i.e. `2015-18`) of the time the crawl occurred.

    ./bin/webindex getpaths 2015-18

Take a look at the paths file that was just retrieved.

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

## Copy Common Crawl data from AWS into HDFS

After retrieving a paths file, the command below runs a Spark job that copies data files from S3 
to HDFS.  The command below will copy 3 files in the file range of `4-6` of the `2015-18` paths 
file into the HDFS directory `/cc/data/a`.  Common Crawl data files are large (~330 MB each) so
be mindful of how many you copy.

    ./bin/webindex copy 2015-18 4-6 /cc/data/a

To create multiple data sets, run the command with different range and HDFS directory.

    ./bin/webindex copy 2015-18 7-8 /cc/data/b

## Initialize the webindex Fluo application

After copying data into HDFS, run the following to initialize and start the webindex
Fluo application.

    ./bin/webindex init

Optionally, add a HDFS directory (with previously copied data) to the end of the command.  
When a directory is specified, `init` will run a Spark job that initializes the webindex
Fluo application with data before starting it.
    
    ./bin/webindex init /cc/data/a

## Start the webindex Fluo application

After the Fluo application has been initialized, pick a method below to run the application:

1. Run local processes:

    fluo oracle -a webindex &> oracle.log
    fluo worker -a webindex &> worker.log

1. Run in YARN:

    fluo-yarn start webindex /path/to/fluo-yarn.properties

1. [Run in Docker](https://fluo.apache.org/docs/fluo/1.2/administration/run-fluo-in-docker)


## Load data into the webindex Fluo application

The `init` command should only be run on an empty cluster.  To add more data, run the 
`load-hdfs` or `load-s3` commands.  Both start a Spark job that parses Common Crawl data 
and inserts this data into the Fluo table of the webindex application.  The webindex Fluo 
observers will incrementally process this data and export indexes to Accumulo.

The `load-hdfs` command below loads data stored in the HDFS directory `/cc/data/b` into 
Fluo.

    ./bin/webindex load-hdfs /cc/data/b

The `load-s3` command below loads data hosted on S3 into Fluo.  It select files in the 
`9-10` range of the `2015-18` paths file.

    ./bin/webindex load-s3 2015-18 9-10

## Compact Transient Ranges

For long runs, this example has [transient ranges][transient] that need to be 
periodically compacted.  This can be accomplished with the following command.

```bash
nohup fluo exec webindex org.apache.fluo.recipes.accumulo.cmds.CompactTransient 600 &> your_log_file.log &
```

As long as this command is running, it will initiate a compaction of all transient 
ranges every 10 minutes.

## Run the webindex UI

Run the following command to run the webindex UI which can be viewed at 
[http://localhost:4567/](http://localhost:4567/).

    ./bin/webindex ui

The UI queries indexes stored in Accumulo that were exported by Fluo.

[Uno]: https://github.com/astralway/uno
[transient]: https://github.com/apache/fluo-recipes/blob/master/docs/transient.md
[cdata]: https://commoncrawl.org/the-data/get-started/

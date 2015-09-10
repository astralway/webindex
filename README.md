# WebIndex

An example Fluo applications that creates a web index using CommonCrawl data.

### Requirements

In order run this application you need the following installed and running on your
machine:

* Hadoop (HDFS & YARN)
* Accumulo
* Fluo

Consider using [fluo-dev] to run these requirments

### Configure your environment

First, you must create `data.yml` and `dropwizard.yml` files and edit them for your environment:

    cd conf
    cp data.yml.example data.yml
    cp dropwizard.yml.example dropwizard.yml

Make sure that `fluoPropsPath` in `data.yml` point the `fluo.properties` file of this application.

### Download CommonCrawl data

Next, run the following command to download CommonCrawl data files.  The data files can have a `fileType`
of `wat`, `wet`, or `warc`.  The command downloads a file containing the URL path of thousands of files and
`numFiles` specifies how many of those files will be downloaded from AWS and loaded into your HDFS instance.

     # Command structure
    ./bin/download.sh <fileType> <numFiles>
     # Use command below for this example
    ./bin/download.sh wat 1

### Initialize Fluo & Accumulo with data

Next, run the following command to run Fluo and initialize it and Accumulo with data:

    ./bin/init.sh

### Run the web application

Finally, run the following command to run the web app:

    ./bin/webapp.sh

Open your browser to [http://localhost:8080/](http://localhost:8080/)

[fluo-dev]: https://github.com/fluo-io/fluo-dev

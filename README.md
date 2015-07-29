# Common Crawl

### Requirements

In order run this application you need the following installed and running on your
machine:

* Hadoop (HDFS & YARN)
* Accumulo
* Fluo

Consider using [fluo-dev] to run these requirments

### Run InboundLinks web application

First, A WAT CommonCrawl data file must downloaded from AWS and loaded into HDFS 
using the following command:

    ./bin/load.sh wat 1

Next, you must create `env.sh` and `web.yml` files and edit them for your environment:

    cd conf
    cp env.sh.example env.sh
    cp web.yml.example web.yml

Run the following commands to create a Fluo application:

    fluo new ccrawl
    fluo init ccrawl
    fluo start ccrawl

Load data into Fluo using Spark:

    ./bin/inbound.sh spark fluo

Finally, run the following command to run the web server:

    ./bin/webserver.sh

Browse to http://localhost:8080/ 

[fluo-dev]: https://github.com/fluo-io/fluo-dev

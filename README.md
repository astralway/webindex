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

Next, you must create `data.yml` and `dropwizard.yml` files and edit them for your environment:

    cd conf
    cp data.yml.example data.yml
    cp dropwizard.yml.example dropwizard.yml

Run the following commands to create a Fluo application:

    fluo new ccrawl
    fluo init ccrawl
    fluo start ccrawl

Make sure that `fluoPropsPath` in `data.yml` point the `fluo.properties` file of this application.

Run the Spark job to initialize Accumulo and Fluo with data:

    ./bin/init.sh

Finally, run the following command to run the web app:

    ./bin/webapp.sh

Browse to http://localhost:8080/ 

[fluo-dev]: https://github.com/fluo-io/fluo-dev

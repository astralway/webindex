![Webindex][logo]
---
[![Build Status][ti]][tl] [![Apache License][li]][ll]

WebIndex is an example [Apache Fluo][fluo] application that uses [Common Crawl][cc] web crawl data
to index links to web pages in multiple ways. It has a simple UI to view the resulting indexes. If
you are new to Fluo, you may want start with the[phrasecount][pc] application as the WebIndex
application is more complicated. For more information on how the WebIndex application works, view
the [tables](docs/tables.md) and [code](docs/code-guide.md) documentation.

## Running WebIndex

If you are new to WebIndex, the simplest way to run the application is to run the development
server. First, clone the WebIndex repo:

    git clone https://github.com/astralway/webindex.git

Next, on a machine where Java and Maven are installed, run the development server using the 
`webindex` command:

    cd webindex/
    ./bin/webindex dev

This will build and start the development server which will log to the console. When you want to
terminate the server, press `ctrl-c`.

The development server starts a MiniAccumuloCluster and runs MiniFluo on top of it.  It parses a
CommonCrawl data file and creates a file at `data/1K-pages.txt` with 1000 pages that are loaded into
MiniFluo. The pages are processed by Fluo which exports indexes to Accumulo. A web application is
started at [http://localhost:4567](http://localhost:4567) that queries these indexes.

If you would like to run WebIndex on a cluster, follow the [install] instructions. 

[fluo]: https://fluo.apache.org/
[pc]: https://github.com/astralway/phrasecount
[cc]: https://commoncrawl.org/
[install]: docs/install.md
[ti]: https://travis-ci.org/astralway/webindex.svg?branch=master
[tl]: https://travis-ci.org/astralway/webindex
[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://github.com/astralway/webindex/blob/master/LICENSE
[logo]: contrib/webindex.png

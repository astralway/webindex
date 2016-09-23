
# Code Guide

The Webindex example has three major code components.

 * Spark component :  Generates initial Fluo and Query [tables].
 * Fluo component :  Updates the [Query table][qt] as web pages are added, removed, and updated.
 * Web component : Web application that uses the [Query table][qt]. 

Since all of these components either read or write the Query table, you may
want to read about the [Query Table][qt] before reading about the code.

## Guide to Fluo Component.

The following image shows a high level view of how data flows through the Fluo
Webindex code.   

<center>![Observer Map](webindex_graphic.png)</center>
<!--
The image was produced using Google Docs.  A link to the source is here.
https://docs.google.com/drawings/d/1vl26uXtScXn1ssj3WEb-qskuH-15OOmWul1B562oWDc/edit?usp=sharing
-->

### Page Loader

The [PageLoader] queues updated page content for processing by the [PageObserver].

### Page Observer

The [PageObserver] computes changes to links within a page by comparing new and current pages. It
computes links added and deleted and then pushes this information to the [UriMap] observer and
[IndexExporter].

Conceptually when a page references a new URI, a `+1` is queued up for the [UriMap].  When a page no
longer references a URI, a `-1` is queued up for the [UriMap] to process.

### URI Map Observer

The [UriMap] observer computes per URI reference counts. The code for this this observer is very
simple because it builds on the Collision Free Map Recipe. A Collision Free Map has two extension
points and this example implements both. The first extension point is a combiner that processes the
`+1` and `-1` updates queued up by the [PageObserver]. The second extension point is an update
observer that handles changes in reference counts for a URI.  It pushes these changes in reference
counts to the [DomainMap] and [IndexExporter].

Changes to URI reference counts are aggregated per domain and `+1` and `-1` updates are queued for
the [DomainMap].

### Domain Map Observer

The [DomainMap] observer computers per domain reference counts. This is a Collision Free Map that
tracks per domain information. When its notified that domain counts changed, it queues updates for
the [IndexExporter] to update the Query table.

### IndexExporter

The [IndexExporter] is an implementation of the 'AccumuloExporter' recipe.  It makes updates to
Accumulo using [IndexUpdate] objects that are placed on a shared 'ExportQueue' by the
[PageObserver], [UriMap], and [DomainMap].

[IndexUpdate] is an interface that is implemented by the following classes:

1. [DomainUpdate] - Updates information related to domain (like page count).

2. [PageUpdate] - Updates information related to page (like links being added or deleted).

3. [UriUpdate] - Updates information related to URI.

When [IndexExporter] receives these objects, it translates the update to mutations using code in
the [IndexClient].


[PageLoader]: ../modules/data/src/main/java/webindex/data/fluo/PageLoader.java
[PageObserver]: ../modules/data/src/main/java/webindex/data/fluo/PageObserver.java
[UriMap]: ../modules/data/src/main/java/webindex/data/fluo/UriMap.java
[DomainMap]: ../modules/data/src/main/java/webindex/data/fluo/DomainMap.java
[IndexExporter]: ../modules/data/src/main/java/webindex/data/fluo/IndexExporter.java
[IndexUpdate]: ../modules/core/src/main/java/webindex/core/models/export/IndexUpdate.java
[DomainUpdate]: ../modules/core/src/main/java/webindex/core/models/export/DomainUpdate.java
[PageUpdate]: ../modules/core/src/main/java/webindex/core/models/export/PageUpdate.java
[UriUpdate]: ../modules/core/src/main/java/webindex/core/models/export/UriUpdate.java
[IndexClient]: ../modules/core/src/main/java/webindex/core/IndexClient.java
[qt]: tables.md#query-table-schema
[tables]: tables.md



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

This loader queues updated page content for processing by the page observer.

**Code:** [PageLoader.java][PageLoader]

### Page Observer

This observer computes changes to links within a page by comparing new and
current pages.  It computes links added and deleted and then pushes this
information to the URI Map Observer and Page Exporter.

Conceptually when a page references a new URI, a `+1` is queued up for the Uri
Map.  When a page no longer references a URI, a `-1` is queued up for the Uri
Map to process.

**Code:** [PageObserver.java][PageObserver]

### URI Map Observer

This observer computes per URI reference counts.  The code for this this
observer is very simple because it builds on the Collision Free Map Recipe.  A
Collision Free Map has two extension points and this example implements both.
The first extension point is a combiner that processes the `+1` and `-1`
updates queued up by the Page Observer.   The second extension point is an
update observer that handles changes in reference counts for a URI.  It pushes
these changes in reference counts to the Domain Map and URI Exporter.

Changes to URI reference counts are aggregated per domain and `+1` and `-1`
updates are queued for the domain map.

**Code:** [UriMap.java][UriMap]

### Domain Map Observer

This observer computers per domain reference counts.  This is a Collision Free
Map that tracks per domain information. When its notified that domain counts
changed, it pushes updates to the export queue to update the Query table.

**Code:** [DomainMap.java][DomainMap]

### Page Exporter

For each URI, the Query table contains the URIs that reference it.  This export
code keeps that information in the Query table up to date.  One interesting
concept this code uses is the concept of inversion on export.  The
complete inverted URI index is never built in Fluo, its only built in Query
table.

**Code:** [PageExport.java][PageExport]

### URI Exporter

Previous observers calculated the total number of URIs that reference a URI.
This export code is given the new and old URI reference counts.  URI reference
counts are indexed three different ways in the Query table.  This export code
updates all three places in the Query table.

This export code also uses the invert on export concept.  The three indexes are
never built in the Fluo table.  Fluo only tracks the minimal amount of
information needed to keep the three indexes current.

**Code:** [UriCountExport.java][UriCountExport]

### Domain Exporter

Export changes to the number of URIs referencing a domain to the Query table.

**Code:** [DomainExport.java][DomainExport]

[PageLoader]: ../modules/data/src/main/java/webindex/data/fluo/PageLoader.java
[PageObserver]: ../modules/data/src/main/java/webindex/data/fluo/PageObserver.java
[UriMap]: ../modules/data/src/main/java/webindex/data/fluo/UriMap.java
[DomainMap]: ../modules/data/src/main/java/webindex/data/fluo/DomainMap.java
[UriCountExport]: ../modules/data/src/main/java/webindex/data/fluo/UriCountExport.java
[PageExport]: ../modules/data/src/main/java/webindex/data/fluo/PageExport.java
[DomainExport]: ../modules/data/src/main/java/webindex/data/fluo/DomainExport.java
[qt]: tables.md#query-table-schema
[tables]: tables.md


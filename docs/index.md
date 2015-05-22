# What is cbft

cbft, or Couchbase Full-Text server, is distributed, clusterable, data
indexing server.

It includes the ability to manage full-text and other kinds of indexes
for JSON documents that you've created and stored into a Couchbase
bucket and other data sources.

The indexes that cbft manages can be automatically distributed across
multiple, clustered cbft server processes on different server machines
to support larger indexes, higher performance and higher availability.

# Developer preview

cbft is still in its infancy and under active development, and is
currently available as a "developer preview" level of quality.

Any feedback provided at this early stage by the early users and the
community are greatly appreciated by the development team.

Now's a great time to affect cbft's features and its directions with
your input!

As a developer preview, some disclaimers and limitations...

* quality level - cbft is not yet production ready quality.

* cbft has not been performance optimized.

* cbft's API are still subject to large, potentially
  backwards-incompatible changes.

* cbft's file storage backends are still subject to change.

* partial index rollbacks are not yet supported by cbft developer
  preview.

# Getting started

## Prerequisites

You should have a Couchbase Server (3.x, 4.x or greater) already
installed and running before trying the rest of these getting started
steps.

If you're not already running Couchbase Server and need to download
it, please see...

* [http://www.couchbase.com/downloads](http://www.couchbase.com/downloads)

You should also have a bucket in Couchbase Server with JSON documents
that you'd like to index.

For example, while during the setup steps of Couchbase Server, you can
have Couchbase Server create and populate a ```beer-sample``` bucket
of sample JSON documents.

## Getting cbft

Download a pre-built cbft from the
[releases](https://github.com/couchbaselabs/cbft/releases) page...

* [https://github.com/couchbaselabs/cbft/releases](https://github.com/couchbaselabs/cbft/releases)

You can use your favorite web browser to download.

Or, you can use ```wget``` or equivalent command-line tool.

For example, for mac OS...

    wget https://github.com/couchbaselabs/cbft/releases/download/v{X.Y.Z}/cbft-v{X.Y.Z-AAA}.macos.amd64.tar.gz

Note: some platforms support both ```cbft-full``` and ```cbft```
builds.

- The ```cbft-full``` builds are currently compiled with some
  platform-specific advanced features (text stemmers, etc).

- The ```cbft``` basic builds are exactly the same across all
  platforms, for deployment consistency.

For the purposes of these getting started steps, let's just download
```cbft``` basic builds.

After downloading, then next uncompress what you downloaded...

    tar -xzf cbft-v{X.Y.Z-AAA}.macos.amd64.tar.gz

On windows, for example, you would use unzip...

    unzip cbft-v{X.Y.Z-AAA}.windows.amd64.exe.zip

A quick way to make sure it worked is to try the command-line help...

    ./cbft.macos.amd64 --help

On windows, for example, you would use...

    cbft.windows.amd64.exe --help

For the rest of this documentation, we'll just refer to the cbft
executable as ```./cbft``` rather than some platform specific name
like ```./cbft.macos.amd64```.

## Running cbft

Start cbft, pointing it to your Couchbase Server as your default
datasource server...

    ./cbft -server http://localhost:8091

On windows, for example...

    cbft -server http://localhost:8091

Note: cbft also defaults to using a directory named "data" as its data
directory, which cbft will create in the current working directory if
it does not exist yet.  You can change the data directory path by
using the ```-dataDir``` command-line parameter.

## The web admin UI

Next, point your web browser to cbft's web admin UI...

    http://localhost:8095

In your web browser, you should see a "Welcome to cbft" page in the
web admin UI.

That welcome page will list all the indexes that you've defined; of
course, there should be no indexes at this point.

## Creating a full-text index

Next, let's create your first full-text index in cbft.

On the Indexes listing page (the "Welcome to cbft" page), click on the
```New Index``` button.

A form should appear where you can define your new index
configuration.

Next, let's fill in the form fields...

### Index Name

Each index needs a unique name.

In the Index Name field, type in a name, such as "test-index".

Only alphanumeric characters, hyphens and underscores are allowed for
index names.  Also, the first character must be a alphabetic
character.

### Index Type

The Index Type specifies what kind of index that cbft will create.

From the Index Type dropdown, choose ```full-text (bleve)```.

As soon as you make an Index Type dropdown selection, some additional,
type-dependent input fields should appear (such as an Index Mapping),
but let's ignore them for now and use the provided defaults.

### Source Type

The Source Type specifies what kind of datasource will be used for the
index.

From the Source Type dropdown, choose ```couchbase```.

As soon as you make a Source Type dropdown selection, some additional
type-dependent input fields (such as Source Name) should
appear.

### Source Name

Since our Source Type is ```couchbase```, the Source Name should be
name of a bucket.

Next, type in your bucket's name into the Source Name field.

For example, to index the "default" bucket from your Couchbase
server, type in a Source Name of "default".

NOTE: if your bucket has a password, you can supply the password by
clicking on the ```Show advanced settings``` checkbox, which will
display the ```Source Params``` JSON textarea.  Then, fill in the
```authUser``` field in the JSON with the name of the Couchbase bucket
and the ```authPassword``` field with the bucket's password.

### Your new index

Finally, click the ```Create Index``` button.

You should see a summary page of your new full-text index.

The ```Document Count``` field on the index summary page is a snapshot
of how many documents have been indexed so far.  You can click on the
```Refresh``` button next to the Document Count in order to see
indexing progress.

## Querying your full-text index

Next, click on the ```Query``` tab.

In the query field, type in a query term.

Hit enter/return to execute your first cbft full-text query!

You should see query results appearing below the query field.

## Using the REST API

You can also use the REST API to access your index.

For example, if your index is named ```myFirstIndex```, here's how you
can use the curl tool to check how many documents are indexed...

    curl http://localhost:8095/api/index/myFirstIndex/count

Here's an example of using curl to query the ```myFirstIndex```...

    curl -XPOST --header Content-Type:text/json \
         -d '{"query":{"size":10,"query":{"query":"your-search-term"}}}' \
         http://localhost:8095/api/index/myFirstIndex/query

## Where to go next

That's about it for getting started.

You can see the other command-line parameters of cbft with the
```-h``` flag...

    ./cbft -h

Additionally, cbft's web admin UI has more screens and features, so be
sure to click around and explore!

Finally, don't forget to take a look at these additional documents...

* [Developer's guide](dev-guide/overview.md)
* [API reference](api-ref.md)
* [Administrator's guide](admin-guide/overview.md)
* [Links to more resources](links.md)

# Providing your feedback

If you need help, would like to contribute feedback, or would like to
talk about the cbft with like-minded folks, please have a look at the
[Couchbase forums](https://forums.couchbase.com/) and the links to
[more resources](links.md).

To report bugs or feature requests, please use the cbft issue tracker
currently hosted at
[github](https://github.com/couchbaselabs/cbft/issues).

---

Copyright (c) 2015 Couchbase, Inc.

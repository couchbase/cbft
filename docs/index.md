# What is cbft

cbft is distributed, clusterable, data indexing server.

It includes the ability to manage full-text and other kinds of indexes
for JSON documents that you've created and stored into a Couchbase
bucket and other data sources.

The indexes that cbft manages can be automatically distributed across
multiple, clustered cbft server processes on different server machines
to support larger indexes, higher performance and higher availability.

# Getting started

## Prerequisites

You should have a Couchbase Server (3.0+) already installed and
running somewhere.

You should also have a bucket in Couchbase Server with JSON documents
that you'd like to index.

For example, you can have Couchbase Server create and populate a
```beer-sample``` bucket of sample JSON documents.

## Getting cbft

Download a pre-built cbft from the
[releases](https://github.com/couchbaselabs/cbft/releases) page.  For
example, for OSX...

    wget https://github.com/couchbaselabs/cbft/releases/download/v{X.Y.Z}/v{X.Y.Z-AAA}_cbft.macos.amd64.tar.gz

Note: some platforms support both ```cbft-full``` and ```cbft```
builds.

- The ```cbft-full``` builds are currently compiled with some
  platform-specific advanced features (text stemmers, etc).

- The ```cbft``` basic builds are exactly the same across all
  platforms, for deployment consistency.

For the purposes of these getting started steps, let's just download
```cbft``` basic builds.

Next, uncompress what you downloaded...

    tar -xzf v{X.Y.Z-AAA}_cbft.macos.amd64.tar.gz

A quick way to make sure it worked is to try the command-line help...

    ./cbft.macos.amd64 --help

For the rest of this documentation, we'll just refer to the cbft
executable as ```./cbft``` rather than some platform specific name
like ```./cbft.macos.amd64```.

## Running cbft

Start cbft, pointing it to your Couchbase Server as its datasource
server...

    ./cbft -server http://localhost:8091

Note: cbft defaults to using a directory named "data" as its data
directory, which it will create in the current working directory if it
does not exist yet.  You can change the data directory path by using
the ```-dataDir``` command-line parameter.

## The web admin UI

Next, point your web browser to cbft's web admin UI...

    http://localhost:8095

In your web browser, you should see a "Welcome to cbft" page in the
web admin UI.

That welcome page will list all the indexes that you've defined; of
course, there should be no indexes at this point.

## Creating a full-text index

On the Indexes listing page (the "Welcome to cbft" page), click on the
```New Index``` button.

A form should appear where you can define your new index
configuration.

Next, let's fill in the form fields...

### Index Name

Each index needs a unique name.

In the Index Name field, type in a name, such as "test-index".

Only alphanumeric characters, hyphens and underscores are allowed for
index names.

### Index Type

The Index Type specifies what kind of index that cbft will create.

From the Index Type dropdown, choose ```full-text (bleve)```.

As soon as you make an Index Type dropdown selection, some additional,
type-dependent input fields should appear (Mapping and Store), but
let's ignore them for now and use the provided defaults.

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

That's about it for getting started.

The web admin UI has more screens and features, so be sure to click
around and explore!

# Where to go next

Please see
the [Developer's Guide](dev-guide/overview.md),
the [API Reference](api-ref.md) and
the [Administrator's Guide](admin-guide/overview.md).

---

Copyright (c) 2015 Couchbase, Inc.

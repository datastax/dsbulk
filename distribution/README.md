# DataStax Bulk Loader Distribution

This module assembles DSBulk's binary distributions in various formats:

1. Archives in tar.gz and zip formats;
2. An executable uber-jar;
3. Aggregated sources and javadoc jars.

## Archives

Archives are available in tar.gz and zip formats. They are the preferred way to download and install
DSBulk.

Each archive include: 

* DSBulk's launch scripts; 
* Required libraries;
* Generated documentation;
* Settings reference;
* Sample configuration files.

DSBulk archives can be manually downloaded from
[DataStax](https://downloads.datastax.com/#bulk-loader).

They can also be downloaded from the command line:

    curl -OL https://downloads.datastax.com/dsbulk/dsbulk.tar.gz

Detailed download and installation instructions can be found
[here](https://docs.datastax.com/en/dsbulk/doc/dsbulk/install/dsbulkInstall.html).

Starting with DSBulk 1.9.0, the binary distribution is also available from Maven Central and can be
downloaded from [this
location](https://repo.maven.apache.org/maven2/com/datastax/oss/dsbulk-distribution/).

## Executable uber-jar

Starting with DSBulk 1.9.0, an executable uber-jar is also available from Maven Central and can be
downloaded from [this
location](https://repo.maven.apache.org/maven2/com/datastax/oss/dsbulk-distribution/).

Running the uber-jar is as simple as:

     java -jar ./dsbulk.jar unload -k ks1 -t table1

While convenient for testing purposes, the uber-jar is not recommended for use in production. Always
prefer the archive-based distribution formats.

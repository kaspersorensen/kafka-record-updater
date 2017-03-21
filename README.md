# kafka-record-updater

[![Build Status: Linux](https://travis-ci.org/kaspersorensen/kafka-record-updater.svg?branch=master)](https://travis-ci.org/kaspersorensen/kafka-record-updater)

A tool for updating Apache Kafka records on disk.

This is particularly useful in situations where one or more records accidentally contains errornious or sensitive information and you must get rid of without purging all the Kafka logs.

## Approach

The tool is designed to run on the individual Kafka broker nodes. It updates records by replacing bytes in the log files. To minimize impact on running brokers that may have active file-cursors, it does not allow changing the size of the records, only substituting the record payload with another.

It requires you to _at least_ configure:

 * Data directory (the Kafka log data directory)
 * Updater name/class, for instance "destroy", which performs the actual record updates on disk.

It traverses Kafka log files, optionally filtered by:

 * Topic
 * Partition number
 * Offset (min and max)

Currently available and built-in updaters:

 * `destroy`: Destroys both key and value by replacing all characters with `*`.
 * `destroy-key`: Like `destroy`, but only for the record key.
 * `destroy-value`: Like `destroy`, but only for the record value.
 * `empty-json`: Replaces the record value with an empty JSON document (`{}`).

Plus, you can add your own by implementing the `RecordUpdater` interface.

## Warnings

The tool is __work in progress__ and has not been field-tested yet on a wide variety of Kafka installations.

The tool must be applied to all nodes in your Kafka cluster since it only handles the replicas available in the local data directory.

## Usage

```
$ java -jar kafka-record-updater-0.2.jar
Option "--data-dir" is required
 --data-dir FILE     : The Apache Kafka log/data directory
 --offset-max N      : A max (inclusive) offset number for records to update
 --offset-min N      : A minimum (inclusive) offset number for records to update
 --partition N       : A specific partition number in which to update records
 --topic VAL         : The topic in which to update records
 --updater-class VAL : Sets the class or name of the updater to apply
```

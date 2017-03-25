# KIP-?? - Record updater tool

## Motivation
As Apache Kafka is being used more and more as a long-term persistent store of events in enterprise systems, a rise emerges to be able to update records, if nothing else as an exceptional measure. One particular important use-case is if information gets published to Kafka that has to be removed because of sensitivity or security concerns. DevOps teams in enterprises will typically struggle to do this without having to purge excessive amounts of data and risk loosing other records in the event store that should be retained.

The purpose is NOT to challenge the principle of immutability that goes into Kafka in general. But certain situations require mutations to happen and we need a last-resort tool for these types of situations. For instance, in EU and Argentina there is the Right To Be Forgotten and should a customer wish to have this effectuated, it may be nescesary to guarantee complete overriding of any personally identifyable information. Another obvious use-case is if a software defect by accident causes password or other secrets to leak into the Kafka logs.

## Proposed Change
Introduce a command-line tool or API in Apache Kafka for editing records identified by topic, partition, offset (or offset-range). The tool or API would modify the data persisted in the Kafka logs and thus ensure that any new read of that data would return the edited information.

To minimize disruption to running brokers, the tool should override Kafka log data on-disk and by replacement of bytes, not removal or insertion into the overall file. This way any file-reading cursors should be unaffected by the changes in the file.

The tool or API should preferably have a way in which to only substitute specific parts of a record. For instance, if a record contains multiple user-attributes, including a password (to be 'erased'), it should be able to substitute only the password attribute value with asterix characters or so.

A prototype tool compatible with the Apache Kafka 0.10 log format is available at https://github.com/kaspersorensen/kafka-record-updater

## New or Changed Public Interfaces
TODO: impact to any of the "compatibility commitments" described above. We want to call these out in particular so everyone thinks about them.

## Migration Plan and Compatibility
TODO: if this feature requires additional support for a no-downtime upgrade describe how that will work

## Rejected Alternatives
Using the purging or compaction options available in Kafka are certainly alternatives. But they are poor alternatives in cases where the persistent log is at the same time kept to retain audit logs or replayability of events.

Another alternative is to mirror the Kafka cluster while filtering out or modifying the messages during this mirroring exercise. This would severely increase infrastructure and management requirements for the data erasure/editing to take place.
package kafka.tools.recordupdater;

import java.io.File;
import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import kafka.tools.recordupdater.DirectoryUpdater.Summary;
import kafka.tools.recordupdater.api.RecordUpdater;
import kafka.tools.recordupdater.updaters.DestroyKeyAndValueRecordUpdater;
import kafka.tools.recordupdater.updaters.DestroyKeyRecordUpdater;
import kafka.tools.recordupdater.updaters.DestroyValueRecordUpdater;
import kafka.tools.recordupdater.updaters.EmptyJsonValueUpdater;

/**
 * The command line entrypoint
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    @Option(name = "--data-dir", usage = "The Apache Kafka log/data directory", required = true)
    private File dataDirectory;

    @Option(name = "--topic", usage = "The topic in which to update records", required = false)
    private String topic;

    @Option(name = "--partition", usage = "A specific partition number in which to update records", required = false)
    private Integer partition;

    @Option(name = "--offset-min", usage = "A minimum (inclusive) offset number for records to update", required = false)
    private Long offsetMin;

    @Option(name = "--offset-max", usage = "A max (inclusive) offset number for records to update", required = false)
    private Long offsetMax;

    @Option(name = "--updater", usage = "Sets the name (short name or class name) of the updater to apply to records", required = true)
    private String updaterClass;

    public static void main(String[] args) throws Exception {
        new Main().run(args);
    }

    public void run(String[] args) throws IOException {
        final CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return;
        }

        final RecordUpdater recordUpdater = createRecordUpdater();

        logger.info("=== Kafka-record-updater ===");
        logger.info("Scanning directory: " + FileUtils.getDisplayPath(dataDirectory));

        final DirectoryUpdater directoryUpdater = new DirectoryUpdater(dataDirectory);
        final Summary summary = directoryUpdater.run(new DirectoryUpdater.Callback() {
            @Override
            public boolean visitPartition(String topicName, int partitionNumber) {
                if (partition != null && partition.intValue() != partitionNumber) {
                    return false;
                }
                if (topic != null && !topic.equals(topicName)) {
                    return false;
                }
                return true;
            }

            @Override
            public boolean visitSegment(File segmentFile) {
                // we may make this configurable in the future
                return true;
            }

            @Override
            public boolean visitRecord(long offset) {
                if (offsetMin != null && offsetMin.longValue() < offset) {
                    return false;
                }
                if (offsetMax != null && offsetMax.longValue() > offset) {
                    return false;
                }
                return true;
            }

            @Override
            public RecordUpdater getRecordUpdater() {
                return recordUpdater;
            }
        });

        logger.info(
                "Done! Summary:\n - {} / {} partitions updated\n - {} / {} segment files updated\n - {} / {} records updated",
                summary.updatedPartitions, summary.visitedPartitions, summary.updatedSegments, summary.visitedSegments,
                summary.updatedRecords, summary.visitedRecords);
    }

    private RecordUpdater createRecordUpdater() {
        switch (updaterClass.trim().replace("-", "").toLowerCase()) {
        case "emptyjson":
            return new EmptyJsonValueUpdater();
        case "destroykey":
        case "destroykeys":
            return new DestroyKeyRecordUpdater();
        case "destroyvalue":
        case "destroyvalues":
            return new DestroyValueRecordUpdater();
        case "destroy":
            return new DestroyKeyAndValueRecordUpdater();
        default:
            try {
                final Object instance = Class.forName(updaterClass).newInstance();
                return (RecordUpdater) instance;
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }
    }
}

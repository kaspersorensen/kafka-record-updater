package kafka.tools.recordupdater;

import java.io.File;
import java.io.IOException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.tools.recordupdater.DirectoryUpdater.Summary;

/**
 * The command line entrypoint
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    @Option(name = "--data-dir", usage = "The Apache Kafka log/data directory", required = true)
    private File dataDirectory;

    @Option(name = "--topic", usage = "The topic in which to update records", required = true)
    private String topic;

    @Option(name = "--partition", usage = "A specific partition number in which to update records", required = false)
    private Integer partition;

    @Option(name = "--updater-class", usage = "Sets the class or name of the updater to apply")
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

        logger.info("=== Kafka-record-updater ===");
        logger.info("Scanning directory: " + FileUtils.getDisplayPath(dataDirectory));

        final DirectoryUpdater directoryUpdater = new DirectoryUpdater(dataDirectory);
        final Summary summary = directoryUpdater.run(new DirectoryUpdater.Callback() {
            @Override
            public boolean visitPartition(String topicName, int partitionNumber) {
                if (partition != null && partition.intValue() != partitionNumber) {
                    return false;
                }
                return topic.equals(topicName);
            }

            @Override
            public boolean visitSegment(File segmentFile) {
                // we may make this configurable in the future
                return true;
            }
        });

        logger.info(
                "Done! Summary:\n - {} / {} partitions updated\n - {} / {} segment files updated\n - {} / {} records updated",
                summary.updatedPartitions, summary.visitedPartitions, summary.updatedSegments, summary.visitedSegments,
                summary.updatedRecords, summary.visitedRecords);
    }
}

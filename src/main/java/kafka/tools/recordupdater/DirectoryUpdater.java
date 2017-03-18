package kafka.tools.recordupdater;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.tools.recordupdater.api.RecordUpdater;

public class DirectoryUpdater {

    private static final Logger logger = LoggerFactory.getLogger(DirectoryUpdater.class);

    public static interface Callback {

        public boolean visitPartition(String topicName, int partitionNumber);

        public boolean visitSegment(File segmentFile);
    }

    public static class Summary {
        public int visitedPartitions = 0;
        public int updatedPartitions = 0;
        public int visitedSegments = 0;
        public int updatedSegments = 0;
        public long visitedRecords = 0;
        public long updatedRecords = 0;
    }

    private final File dir;

    public DirectoryUpdater(File dir) {
        if (dir == null) {
            throw new IllegalArgumentException("Directory cannot be null");
        }
        if (!dir.exists()) {
            throw new IllegalArgumentException("Directory does not exist: " + FileUtils.getDisplayPath(dir));
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + FileUtils.getDisplayPath(dir));
        }
        if (!new File(dir, "meta.properties").exists()) {
            throw new IllegalArgumentException("Directory is not a Kafka data directory (no 'meta.properties' file): "
                    + FileUtils.getDisplayPath(dir));
        }
        this.dir = dir;
    }

    public Summary run(Callback callback) throws IOException {
        final Summary s = new Summary();
        final File[] partitionDirectories = dir.listFiles(createPartitionDirectoryFilter(callback));
        for (File partitionDirectory : partitionDirectories) {
            s.visitedPartitions++;
            logger.info("Visiting partition: {}", FileUtils.getDisplayPath(partitionDirectory));

            final boolean updated = updatePartitionDirectory(partitionDirectory, callback, s);
            if (updated) {
                s.updatedPartitions++;
            }
        }
        return s;
    }

    private boolean updatePartitionDirectory(File partitionDirectory, Callback callback, Summary s) throws IOException {
        boolean partitionUpdated = false;
        final File[] segmentFiles = partitionDirectory.listFiles(createSegmentFileFilter());
        for (File segmentFile : segmentFiles) {
            s.visitedSegments++;

            final LogFileUpdater logFileUpdater = new LogFileUpdater(segmentFile);
            final boolean segmentUpdated = logFileUpdater.run(new RecordUpdater() {
                @Override
                public boolean update(long offset, byte[] key, byte[] value) {
                    s.visitedRecords++;

                    return false;
                }
            });
            if (segmentUpdated) {
                s.updatedSegments++;
                partitionUpdated = true;
            }
        }
        return partitionUpdated;
    }

    private FilenameFilter createSegmentFileFilter() {
        return new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".log");
            }
        };
    }

    private FileFilter createPartitionDirectoryFilter(Callback callback) {
        return new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (!file.isDirectory()) {
                    return false;
                }
                final int lastIndexOfDash = file.getName().lastIndexOf('-');
                if (lastIndexOfDash == -1) {
                    return false;
                }

                // the prefix will be the topic name
                final String prefix = file.getName().substring(0, lastIndexOfDash);
                // any kafka log directory ends with a dash and a partition
                // number
                final String suffix = file.getName().substring(lastIndexOfDash + 1);
                final int partitionNumber;
                try {
                    partitionNumber = Integer.parseInt(suffix);
                } catch (NumberFormatException e) {
                    return false;
                }
                return callback.visitPartition(prefix, partitionNumber);
            }
        };
    }
}

package kafka.tools.recordupdater;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Arrays;

import org.junit.Test;

import com.google.common.io.Files;

import kafka.tools.recordupdater.api.RecordUpdater;
import kafka.tools.recordupdater.updaters.DestroyValueRecordUpdater;

public class SegmentFileUpdaterTest {

    @Test
    public void testTraverseFileDespiteCrcIssues() throws Exception {
        final File file = new File("src/test/resources/example-log-health-check.log");
        final SegmentFileUpdater updater = new SegmentFileUpdater(file, false);
        updater.run(new RecordUpdater() {
            @Override
            public boolean update(long offset, byte[] key, byte[] value) {
                assertEquals("192.168.99.1", new String(key));
                return false;
            }
        });
    }

    @Test
    public void testGetLong() {
        assertEquals(16, SegmentFileUpdater.getLong(new byte[] { 16 }));
        assertEquals(16, SegmentFileUpdater.getLong(new byte[] { 0, 16 }));
        assertEquals(16, SegmentFileUpdater.getLong(new byte[] { 0, 0, 0, 0, 16 }));
        assertEquals(149, SegmentFileUpdater.getLong(new byte[] { 0, -107 }));
        assertEquals(129, SegmentFileUpdater.getLong(new byte[] { -127 }));
        assertEquals(255, SegmentFileUpdater.getLong(new byte[] { -1 }));
        assertEquals(256, SegmentFileUpdater.getLong(new byte[] { 1, 0 }));
        assertEquals(272, SegmentFileUpdater.getLong(new byte[] { 1, 16 }));
        assertEquals(528, SegmentFileUpdater.getLong(new byte[] { 2, 16 }));
    }

    @Test
    public void testGetBytes() {
        assertEquals("[16]", Arrays.toString(SegmentFileUpdater.getBytes(16, 1)));
        assertEquals("[0, 16]", Arrays.toString(SegmentFileUpdater.getBytes(16, 2)));
        assertEquals("[0, 0, 0, 0, 16]", Arrays.toString(SegmentFileUpdater.getBytes(16, 5)));
        assertEquals("[0, -107]", Arrays.toString(SegmentFileUpdater.getBytes(149, 2)));
        assertEquals("[-1]", Arrays.toString(SegmentFileUpdater.getBytes(255, 1)));
        assertEquals("[1, 0]", Arrays.toString(SegmentFileUpdater.getBytes(256, 2)));
        assertEquals("[1, 16]", Arrays.toString(SegmentFileUpdater.getBytes(272, 2)));
        assertEquals("[2, 16]", Arrays.toString(SegmentFileUpdater.getBytes(528, 2)));
    }

    @Test
    public void testCalculateCrc() throws Exception {
        final File workingFile = new File("target/testCalculateCrc.log");
        Files.copy(new File("src/test/resources/example-log-dir-hello/hello-0/00000000000000000000.log"), workingFile);

        final SegmentFileUpdater verifier1 = new SegmentFileUpdater(workingFile, true);
        // first verify integrity of original file
        verifier1.run(null);

        final SegmentFileUpdater updater = new SegmentFileUpdater(workingFile, true);
        updater.run(new DestroyValueRecordUpdater('!'));

        // re-run verification
        final SegmentFileUpdater verifier2 = new SegmentFileUpdater(workingFile, true);
        verifier2.run(new RecordUpdater() {
            @Override
            public boolean update(long offset, byte[] key, byte[] value) {
                assertEquals('!', value[0]);
                return false;
            }
        });
    }
}

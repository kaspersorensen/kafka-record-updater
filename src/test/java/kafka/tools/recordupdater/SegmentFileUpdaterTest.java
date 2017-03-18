package kafka.tools.recordupdater;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

import kafka.tools.recordupdater.SegmentFileUpdater;
import kafka.tools.recordupdater.api.RecordUpdater;

public class SegmentFileUpdaterTest {

    @Test
    public void testTraverseFile() throws Exception {
        File file = new File("src/test/resources/example-log-health-check.log");
        SegmentFileUpdater updater = new SegmentFileUpdater(file);
        updater.run(new RecordUpdater() {
            @Override
            public boolean update(long offset, byte[] key, byte[] value) {
                assertEquals("192.168.99.1", new String(key));
                return false;
            }
        });
    }

    @Test
    public void testGetLength() {
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
}

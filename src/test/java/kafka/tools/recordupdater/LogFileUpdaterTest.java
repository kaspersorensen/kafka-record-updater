package kafka.tools.recordupdater;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

import kafka.tools.recordupdater.LogFileUpdater;

public class LogFileUpdaterTest {

    @Test
    public void testTraverseFile() throws Exception {
        File file = new File("src/test/resources/example-log-health-check.log");
        LogFileUpdater updater = new LogFileUpdater(file);
        updater.run(new LogFileUpdater.Callback() {
            @Override
            public boolean update(int offset, byte[] key, byte[] value) {
                assertEquals("192.168.99.1", new String(key));
                return false;
            }
        });
    }

    @Test
    public void testGetLength() {
        assertEquals(16, LogFileUpdater.getInteger(new byte[] { 16 }));
        assertEquals(16, LogFileUpdater.getInteger(new byte[] { 0, 16 }));
        assertEquals(16, LogFileUpdater.getInteger(new byte[] { 0, 0, 0, 0, 16 }));
        assertEquals(149, LogFileUpdater.getInteger(new byte[] { 0, -107 }));
        assertEquals(129, LogFileUpdater.getInteger(new byte[] { -127 }));
        assertEquals(255, LogFileUpdater.getInteger(new byte[] { -1 }));
        assertEquals(256, LogFileUpdater.getInteger(new byte[] { 1, 0 }));
        assertEquals(272, LogFileUpdater.getInteger(new byte[] { 1, 16 }));
        assertEquals(528, LogFileUpdater.getInteger(new byte[] { 2, 16 }));
    }
}

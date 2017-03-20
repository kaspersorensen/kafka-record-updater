package kafka.tools.recordupdater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.IntMath;
import com.google.common.primitives.UnsignedBytes;

import kafka.tools.recordupdater.api.RecordUpdater;

/**
 * Updater object capable of traversing through a Kafka log file and performing
 * updates to the bytes that make up a message.
 * 
 * For reference, here's the message format as per the Kafka docs:
 * 
 * <pre>
 * On-disk format of a message
 *
 * offset         : 8 bytes 
 * message length : 4 bytes (value: 4 + 1 + 1 + 8(if magic value > 0) + 4 + K + 4 + V)
 * crc            : 4 bytes
 * magic value    : 1 byte
 * attributes     : 1 byte
 * timestamp      : 8 bytes (Only exists when magic value is greater than zero)
 * key length     : 4 bytes
 * key            : K bytes
 * value length   : 4 bytes
 * value          : V bytes
 * </pre>
 * 
 * @author Kasper Sørensen
 */
public class SegmentFileUpdater {

    private static final Logger logger = LoggerFactory.getLogger(SegmentFileUpdater.class);

    private final File file;

    private final int LENGTH_BYTES = 4;

    private final byte[] messageOffset = new byte[8];
    private final byte[] messageLength = new byte[4];
    private final byte[] messageCrc = new byte[4];
    private final byte[] messageTimestamp = new byte[8];
    private final byte[] messageKeyLength = new byte[LENGTH_BYTES];
    private final byte[] messageValueLength = new byte[LENGTH_BYTES];
    private byte messageMagicValue;

    public SegmentFileUpdater(File file) {
        this.file = file;
    }

    public boolean run(RecordUpdater recordUpdater) throws FileNotFoundException, IOException {
        long recordsVisited = 0;
        long recordsUpdated = 0;

        try (final RandomAccessFile raf = new RandomAccessFile(file, "rwd")) {
            while (true) {
                // populate bytes
                if (raf.read(messageOffset) == -1) {
                    // eof
                    break;
                }
                recordsVisited++;

                final long offset = getLong(messageOffset);

                raf.read(messageLength);
                raf.read(messageCrc);
                messageMagicValue = (byte) raf.read();

                // read past the 'attributes' byte which we don't care about
                raf.read();

                if (messageMagicValue > 0) {
                    raf.read(messageTimestamp);
                }
                raf.read(messageKeyLength);

                final int keyLength = getInteger(messageKeyLength);
                final byte[] messageKey;
                if (keyLength == -1) {
                    messageKey = new byte[0];
                } else {
                    messageKey = new byte[keyLength];
                    raf.readFully(messageKey);
                }
                
                raf.readFully(messageValueLength);
                final int valueLength = getInteger(messageValueLength);
                final byte[] messageValue = new byte[valueLength];
                raf.readFully(messageValue);

                if (recordUpdater.update(offset, messageKey, messageValue)) {
                    final long filePointer = raf.getFilePointer();
                    final long messageValueOffset = filePointer - valueLength;
                    final long messageKeyOffset = messageValueOffset - LENGTH_BYTES - keyLength;
                    
                    // TODO: Update CRC - "The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer."

                    raf.seek(messageKeyOffset);
                    raf.write(messageKey);
                    raf.seek(messageValueOffset);
                    raf.write(messageValue);

                    logger.debug("Updated record with offset={} in segment file: {}", offset, file);

                    recordsUpdated++;
                }
            }
        }
        logger.info("Updated {} / {} records in segment file: {}", recordsUpdated, recordsVisited, file);
        return recordsUpdated > 0;
    }

    protected static long getLong(byte[] b) {
        long sum = 0;
        for (int i = 0; i < b.length; i++) {
            int reverseIndex = b.length - 1 - i;
            int factor = IntMath.pow(256, reverseIndex);
            sum += UnsignedBytes.toInt(b[i]) * factor;
        }
        return sum;
    }

    protected static int getInteger(byte[] b) {
        int sum = 0;
        for (int i = 0; i < b.length; i++) {
            int reverseIndex = b.length - 1 - i;
            int factor = IntMath.pow(256, reverseIndex);
            sum += UnsignedBytes.toInt(b[i]) * factor;
        }
        return sum;
    }
}

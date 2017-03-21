package kafka.tools.recordupdater;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
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
 * message length : 4 bytes (value: 4 + 1 + 1 + 8(if magic value &gt; 0) + 4 + K + 4 + V)
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
    private final boolean verifyCrc;
    private long recordsVisited = 0;
    private long recordsUpdated = 0;

    public SegmentFileUpdater(File file) {
        this(file, false);
    }

    public SegmentFileUpdater(File file, boolean verifyCrc) {
        this.file = file;
        this.verifyCrc = verifyCrc;
    }

    public boolean run(RecordUpdater recordUpdater) throws FileNotFoundException, IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rwd")) {
            while (true) {
                try {
                    if (handleNextRecord(recordUpdater, raf)) {
                        recordsVisited++;
                    } else {
                        break;
                    }
                } catch (EOFException e) {
                    logger.warn("Unexpected EOF at record no. {} in {}", recordsVisited + 1, file);
                    break;
                }
            }
        }
        logger.info("Updated {} / {} records in segment file: {}", recordsUpdated, recordsVisited, file);
        return recordsUpdated > 0;
    }

    public long getRecordsUpdated() {
        return recordsUpdated;
    }

    public long getRecordsVisited() {
        return recordsVisited;
    }

    private boolean handleNextRecord(RecordUpdater recordUpdater, RandomAccessFile raf) throws IOException {
        // populate bytes
        if (raf.read(messageOffset) == -1) {
            // eof
            return false;
        }

        final long offset = getLong(messageOffset);

        raf.read(messageLength);

        final long crcOffset = raf.getFilePointer();
        raf.read(messageCrc);
        messageMagicValue = (byte) raf.read();

        // read past the 'attributes' byte which we don't care about
        byte attributes = (byte) raf.read();

        if (messageMagicValue > 0) {
            raf.read(messageTimestamp);
        }
        raf.read(messageKeyLength);

        final long messageKeyOffset = raf.getFilePointer();

        final int keyLength = getInteger(messageKeyLength);
        final byte[] messageKey;
        if (keyLength == -1) {
            messageKey = new byte[0];
        } else {
            messageKey = new byte[keyLength];
            raf.readFully(messageKey);
        }

        raf.readFully(messageValueLength);
        final long messageValueOffset = raf.getFilePointer();

        final int valueLength = getInteger(messageValueLength);
        final byte[] messageValue = new byte[valueLength];
        raf.readFully(messageValue);

        final long existingCrc = getLong(messageCrc);

        if (verifyCrc) {
            // could be turned on for verification
            final long calculatedCrc = calculateCrc(attributes, messageKey, messageValue);
            if (existingCrc != calculatedCrc) {
                throw new IllegalStateException("Invalid CRC value detected. Expected " + calculatedCrc
                        + " (calculated) but found " + existingCrc + " (in file)");
            }
        }

        if (recordUpdater != null && recordUpdater.update(offset, messageKey, messageValue)) {
            // Update CRC
            final long newCrc = calculateCrc(attributes, messageKey, messageValue);
            if (existingCrc != newCrc) {
                raf.seek(crcOffset);
                raf.write(getBytes(newCrc, messageCrc.length));
            }

            raf.seek(messageKeyOffset);
            raf.write(messageKey);
            raf.seek(messageValueOffset);
            raf.write(messageValue);

            logger.debug("Updated record with offset={} in segment file: {}", offset, file);

            recordsUpdated++;
        }
        return true;
    }

    /**
     * Calculates the CRC of the current message.
     * 
     * From the Kafka documentation: "The CRC is the CRC32 of the remainder of
     * the message bytes. This is used to check the integrity of the message on
     * the broker and consumer."
     * 
     * @param attributes
     * @param messageKey
     * @param messageValue
     * @return
     */
    private long calculateCrc(byte attributes, final byte[] messageKey, final byte[] messageValue) {
        final CRC32 crc = new CRC32();
        crc.update(messageMagicValue);
        crc.update(attributes);
        if (messageMagicValue > 0) {
            crc.update(messageTimestamp);
        }
        crc.update(messageKeyLength);
        crc.update(messageKey);
        crc.update(messageValueLength);
        crc.update(messageValue);
        return crc.getValue();
    }

    protected static byte[] getBytes(long sum, int arraySize) {
        final byte[] b = new byte[arraySize];
        for (int i = 0; i < b.length; i++) {
            final int reverseIndex = b.length - 1 - i;
            final long factor = LongMath.pow(256, reverseIndex);
            final long base = sum / factor;
            b[i] = UnsignedBytes.checkedCast(base);
            sum = sum - (base * factor);
        }
        return b;
    }

    protected static long getLong(byte[] b) {
        long sum = 0;
        for (int i = 0; i < b.length; i++) {
            final int reverseIndex = b.length - 1 - i;
            final long factor = LongMath.pow(256, reverseIndex);
            final int base = UnsignedBytes.toInt(b[i]);
            sum += base * factor;
        }
        return sum;
    }

    protected static int getInteger(byte[] b) {
        int sum = 0;
        for (int i = 0; i < b.length; i++) {
            final int reverseIndex = b.length - 1 - i;
            final int factor = IntMath.pow(256, reverseIndex);
            final int base = UnsignedBytes.toInt(b[i]);
            sum += base * factor;
        }
        return sum;
    }
}

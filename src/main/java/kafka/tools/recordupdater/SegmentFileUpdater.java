package kafka.tools.recordupdater;

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
    private final boolean verifyCrc;

    public SegmentFileUpdater(File file) {
        this(file, false);
    }

    public SegmentFileUpdater(File file, boolean verifyCrc) {
        this.file = file;
        this.verifyCrc = verifyCrc;
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
                byte attributes = (byte) raf.read();

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

                final long existingCrc = getLong(messageCrc);

                if (verifyCrc) {
                    // could be turned on for verification
                    final long newCrc = calculateCrc(attributes, messageKey, messageValue);
                    final long calculatedCrc = newCrc;
                    if (existingCrc != calculatedCrc) {
                        throw new IllegalStateException("Invalid CRC value detected. Expected " + calculatedCrc
                                + " (calculated) but found " + existingCrc + " (in file)");
                    }
                }

                if (recordUpdater.update(offset, messageKey, messageValue)) {
                    final long filePointer = raf.getFilePointer();
                    final long messageValueOffset = filePointer - valueLength;
                    final long messageKeyOffset = messageValueOffset - LENGTH_BYTES - keyLength;

                    // Update CRC
                    final long newCrc = calculateCrc(attributes, messageKey, messageValue);
                    if (existingCrc != newCrc) {
                        final long crcOffset = getCrcOffset(messageKeyOffset);
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
            }
        }
        logger.info("Updated {} / {} records in segment file: {}", recordsUpdated, recordsVisited, file);
        return recordsUpdated > 0;
    }

    private long getCrcOffset(long messageKeyOffset) {
        int sub =
                // crc length
                4 +
                // magic value and attribute
                        2 +
                        // timestamp (if available)
                        (messageMagicValue > 0 ? messageTimestamp.length : 0) +
                        // message key length
                        messageKeyLength.length;
        return messageKeyOffset + sub;
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

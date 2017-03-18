package kafka.tools.recordupdater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

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
public class LogFileUpdater {

    private final File file;

    private final int LENGTH_BYTES = 4;

    private final byte[] messageOffset = new byte[8];
    private final byte[] messageLength = new byte[4];
    private final byte[] messageCrc = new byte[4];
    private final byte[] messageTimestamp = new byte[8];
    private final byte[] messageKeyLength = new byte[LENGTH_BYTES];
    private final byte[] messageValueLength = new byte[LENGTH_BYTES];
    private byte messageMagicValue;

    public LogFileUpdater(File file) {
        this.file = file;
    }

    public boolean run(RecordUpdater recordUpdater) throws FileNotFoundException, IOException {
        boolean segmentUpdated = false;
        try (final RandomAccessFile raf = new RandomAccessFile(file, "rwd")) {
            while (true) {
                // populate bytes
                if (raf.read(messageOffset) == -1) {
                    // eof
                    break;
                }

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
                final byte[] messageKey = new byte[keyLength];
                raf.readFully(messageKey);

                raf.readFully(messageValueLength);
                final int valueLength = getInteger(messageValueLength);
                final byte[] messageValue = new byte[valueLength];
                raf.readFully(messageValue);

                if (recordUpdater.update(offset, messageKey, messageValue)) {
                    final long filePointer = raf.getFilePointer();
                    final long messageValueOffset = filePointer - valueLength;
                    final long messageKeyOffset = messageValueOffset - LENGTH_BYTES - keyLength;

                    raf.seek(messageKeyOffset);
                    raf.write(messageKey);
                    raf.seek(messageValueOffset);
                    raf.write(messageValue);
                    segmentUpdated = true;
                }
            }
        }
        return segmentUpdated;
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

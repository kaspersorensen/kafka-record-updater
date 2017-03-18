package kafka.tools.recordupdater.api;

public interface RecordUpdater {

    /**
     * Performs updates on a message, if applicable.
     * 
     * @param offset
     *            the message offset
     * @param key
     *            the message key
     * @param value
     *            the message value
     * @return if either of the arrays have been modified. When true, bytes will
     *         be overwritten in the Kafka log file.
     */
    public boolean update(long offset, byte[] key, byte[] value);
}

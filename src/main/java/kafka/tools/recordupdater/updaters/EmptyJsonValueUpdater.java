package kafka.tools.recordupdater.updaters;

import kafka.tools.recordupdater.api.RecordUpdater;

/**
 * A {@link RecordUpdater} that replaces the value of the record with a blank
 * JSON document ({}).
 */
public class EmptyJsonValueUpdater implements RecordUpdater {

    @Override
    public boolean update(long offset, byte[] key, byte[] value) {
        if (value != null && value.length > 1) {
            for (int i = 0; i < value.length; i++) {
                if (i == 0) {
                    value[i] = '{';
                } else if (i == value.length - 1) {
                    value[i] = '}';
                } else {
                    value[i] = ' ';
                }
            }
            return true;
        }
        return false;
    }

}

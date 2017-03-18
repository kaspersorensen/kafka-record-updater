package kafka.tools.recordupdater.updaters;

import kafka.tools.recordupdater.api.RecordUpdater;

public class DestroyKeyAndValueRecordUpdater extends AbstractDestroyRecordUpdater implements RecordUpdater {

    public DestroyKeyAndValueRecordUpdater() {
        super();
    }

    public DestroyKeyAndValueRecordUpdater(char blankChar) {
        super(blankChar);
    }

    @Override
    public boolean update(long offset, byte[] key, byte[] value) {
        final boolean destroyKey = destroy(key);
        final boolean destroyValue = destroy(value);
        return destroyKey || destroyValue;
    }
}

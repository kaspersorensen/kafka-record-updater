package kafka.tools.recordupdater.updaters;

import kafka.tools.recordupdater.api.RecordUpdater;

public class DestroyValueRecordUpdater extends AbstractDestroyRecordUpdater implements RecordUpdater {

    public DestroyValueRecordUpdater() {
        super();
    }

    public DestroyValueRecordUpdater(char blankChar) {
        super(blankChar);
    }

    @Override
    public boolean update(long offset, byte[] key, byte[] value) {
        return destroy(value);
    }
}

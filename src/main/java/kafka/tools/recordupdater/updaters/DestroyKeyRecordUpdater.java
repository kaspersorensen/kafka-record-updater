package kafka.tools.recordupdater.updaters;

import kafka.tools.recordupdater.api.RecordUpdater;

public class DestroyKeyRecordUpdater extends AbstractDestroyRecordUpdater implements RecordUpdater {

    public DestroyKeyRecordUpdater() {
        super();
    }

    public DestroyKeyRecordUpdater(char blankChar) {
        super(blankChar);
    }

    @Override
    public boolean update(long offset, byte[] key, byte[] value) {
        return destroy(key);
    }
}

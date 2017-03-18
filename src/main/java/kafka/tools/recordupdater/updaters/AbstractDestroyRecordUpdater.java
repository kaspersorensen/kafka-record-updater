package kafka.tools.recordupdater.updaters;

import kafka.tools.recordupdater.api.RecordUpdater;

public abstract class AbstractDestroyRecordUpdater implements RecordUpdater {

    private byte blankChar;

    public AbstractDestroyRecordUpdater() {
        final String blankCharString = System.getProperty("blank.char", "*");
        this.blankChar = (byte) blankCharString.charAt(0);
    }

    public AbstractDestroyRecordUpdater(char blankChar) {
        this.blankChar = (byte) blankChar;
    }

    public boolean destroy(byte[] v) {
        if (v == null || v.length == 0) {
            return false;
        }
        for (int i = 0; i < v.length; i++) {
            v[i] = blankChar;
        }
        return true;
    }
}

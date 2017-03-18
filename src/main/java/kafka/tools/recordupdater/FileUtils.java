package kafka.tools.recordupdater;

import java.io.File;
import java.io.IOException;

public class FileUtils {

    private FileUtils() {
    }

    public static String getDisplayPath(File dir) {
        try {
            return dir.getCanonicalPath();
        } catch (IOException e) {
            return dir.getAbsolutePath();
        }
    }
}

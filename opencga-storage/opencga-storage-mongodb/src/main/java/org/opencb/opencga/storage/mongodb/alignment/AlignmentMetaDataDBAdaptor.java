package org.opencb.opencga.storage.mongodb.alignment;

/**
 * Created by jacobo on 15/08/14.
 */

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * TODO: In future, this will call to the metadata DB
 * <p/>
 * Takes the index from a properties file where "key" is the index, and "value" is the path to the file.
 * If missing, it's created in "/tmp/files-index.properties"
 */
public class AlignmentMetaDataDBAdaptor {
    private Properties bamFiles;
    private String bamFilesPath;
    public AlignmentMetaDataDBAdaptor(String path) {
        bamFilesPath = path;
        bamFiles = new Properties();
        try {
            bamFiles.load(new InputStreamReader(new FileInputStream(bamFilesPath)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Path getBamFromIndex(String index) {
        Path p = Paths.get(bamFiles.getProperty(index));
        if (p.toFile().exists()) {
            return p;
        } else {
            return null;
        }
    }

    public Path getBaiFromIndex(String index) {
        Path p = Paths.get(bamFiles.getProperty(index)+".bai");
        if (p.toFile().exists()) {
            return p;
        } else {
            return null;
        }
    }
    //If path already exists, it is not stored.
    public String registerPath(Path path) {
        String p = path.toAbsolutePath().toString();
        if (bamFiles.containsValue(p)) {
            return null;//bamFiles.getProperty(p);
        } else {
            String index = Integer.toString(bamFiles.size());
            bamFiles.setProperty(index, p);
            try {
                bamFiles.store(new FileOutputStream(bamFilesPath), "BamFiles Index");
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            return index;
        }
    }
}
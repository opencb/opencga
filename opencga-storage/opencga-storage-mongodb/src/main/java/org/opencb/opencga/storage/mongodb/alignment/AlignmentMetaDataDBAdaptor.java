/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * <p>
 * Takes the index from a properties file where "key" is the index, and "defaultValue" is the path to the file.
 * If missing, it's created in "/tmp/files-index.properties"
 */
@Deprecated
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
        Path p = Paths.get(bamFiles.getProperty(index) + ".bai");
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
            return null; //bamFiles.getProperty(p);
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

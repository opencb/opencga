/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 03/10/14.
 */
public class IOManagerFactory {

    protected static Logger logger = LoggerFactory.getLogger(IOManagerFactory.class);
    private String defaultScheme = "file";
    private Map<String, IOManager> ioManagers = new HashMap<>();

    public IOManager get(URI uri) throws IOException {
        return get(uri.getScheme());
    }

    public IOManager getDefault() throws IOException {
        return get(defaultScheme);
    }

    public IOManager get(String io) throws IOException {
        if (io == null) {
            io = defaultScheme;
        }

        if (!ioManagers.containsKey(io)) {
            switch (io) {
                case "file":
                    ioManagers.put("file", new PosixIOManager());
                    break;
//                case "hdfs":
//                    IOManagers.put("hdfs", new HdfsIOManager(configuration));
//                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported file system : " + io);
            }
        }
        return ioManagers.get(io);
    }

    public String getDefaultScheme() {
        return defaultScheme;
    }

    public void setDefaultScheme(String defaultScheme) {
        this.defaultScheme = defaultScheme;
    }

}

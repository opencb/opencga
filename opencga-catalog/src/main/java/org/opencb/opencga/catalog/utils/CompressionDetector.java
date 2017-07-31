/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.catalog.utils;

import com.google.common.io.Files;
import org.opencb.opencga.catalog.models.File;

import java.net.URI;

/**
 * Created by hpccoll1 on 26/05/15.
 */
public class CompressionDetector {

    public static File.Compression detect(URI uri) {
        String fileExtension = Files.getFileExtension(uri.getPath());
        return getCompression(fileExtension);
    }

    public static File.Compression getCompression(String fileExtension) {
        File.Compression compression;

        // Check if fileExtension is null
        if (fileExtension == null) {
            return File.Compression.NONE;
        }

        switch (fileExtension.toLowerCase()) {
            case "gz":
            case "gzip":
                compression = File.Compression.GZIP;
                break;
            case "zip":
                compression = File.Compression.ZIP;
                break;
            case "snappy":
            case "snz":
                compression = File.Compression.SNAPPY;
                break;
            default:
                compression = File.Compression.NONE;
                break;
        }
        return compression;
    }
}

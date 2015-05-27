package org.opencb.opencga.analysis.files;

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
        switch (fileExtension.toLowerCase()) {
            case "gz":
            case "gzip":
                return File.Compression.GZIP;
            case "zip":
                return File.Compression.ZIP;
            case "snappy":
            case "snz":
                return File.Compression.SNAPPY;
        }

        return File.Compression.NONE;
    }
}
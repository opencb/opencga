package org.opencb.opencga.analysis;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class ResourceUtils {

    public static void download(URL url, Path outDir) throws IOException {
        InputStream in = url.openStream();
        Files.copy(in, outDir.resolve(new File(url.getFile()).getName()), StandardCopyOption.REPLACE_EXISTING);
    }

    public static void download(URL url, File outFile) throws IOException {
        InputStream in = url.openStream();
        Files.copy(in, outFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }
}

package org.opencb.opencga.analysis.files;

import org.opencb.opencga.catalog.models.File;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FormatDetector {

    protected static final Map<File.Format, Pattern> formatMap = new HashMap<>();

    static {
        formatMap.put(File.Format.GZIP, Pattern.compile(".*\\.(gzip|gz)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));     //GZIP //TODO: Snappy?
        formatMap.put(File.Format.IMAGE, Pattern.compile(".*\\.(png|jpg|bmp|svg|gif|jpeg|tfg)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));//IMAGE
    }

    public static File.Format detect(URI uri) {

        for (Map.Entry<File.Format, Pattern> entry : formatMap.entrySet()) {
            if (entry.getValue().matcher(uri.getPath()).matches()) {
                return entry.getKey();
            }
        }

        //BINARY
        //TODO

        //EXECUTABLE
        //TODO

        //PLAIN
        return File.Format.PLAIN;
    }


}

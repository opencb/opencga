package org.opencb.opencga.analysis.files;

import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.IOUtils;

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
        formatMap.put(File.Format.IMAGE, Pattern.compile(".*\\.(png|jpg|bmp|svg|gif|jpeg|tfg)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));//IMAGE
    }

    /**
     *
     * @param uri       Existing file uri to the file
     * @return File.Format. UNKNOWN if can't detect any format.
     */
    public static File.Format detect(URI uri) {
        for (Map.Entry<File.Format, Pattern> entry : formatMap.entrySet()) {
            if (entry.getValue().matcher(uri.getPath()).matches()) {
                return entry.getKey();
            }
        }

        String path = uri.getPath();
        String extension = com.google.common.io.Files.getFileExtension(path);
        if (CompressionDetector.getCompression(extension) != File.Compression.NONE) {
            path = com.google.common.io.Files.getNameWithoutExtension(path);
            extension = com.google.common.io.Files.getFileExtension(path);
        }

        switch (extension.toLowerCase()) {
            case "vcf":
                return File.Format.VCF;
            case "bcf":
                return File.Format.BCF;
            case "bam":
                return File.Format.BAM;
            case "bai":
                return File.Format.BAI;
            case "sam":
                return File.Format.SAM;
            case "cram":
                return File.Format.CRAM;
            case "ped":
                return File.Format.PED;
            case "fastq":
                return File.Format.FASTQ;
            case "tsv":
                return File.Format.TAB_SEPARATED_VALUES;
            case "csv":
                return File.Format.COMMA_SEPARATED_VALUES;
            case "txt":
            case "log":
                return File.Format.PLAIN;
            case "xml":
                return File.Format.XML;
            case "json":
                return File.Format.JSON;
            case "proto":
                return File.Format.PROTOCOL_BUFFER;
            case "avro":
                return File.Format.AVRO;
            case "parquet":
                return File.Format.PARQUET;
            case "png":
            case "bmp":
            case "svg":
            case "gif":
            case "jpeg":
            case "tif":
                return File.Format.IMAGE;
        }

        //PLAIN
        return File.Format.UNKNOWN;
    }


}

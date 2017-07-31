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

import org.opencb.opencga.catalog.models.File;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FormatDetector {

    private static final Map<File.Format, Pattern> FORMAT_MAP = new HashMap<>();

    static {
        FORMAT_MAP.put(File.Format.IMAGE, Pattern.compile(".*\\.(png|jpg|bmp|svg|gif|jpeg|tfg)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));
    }

    /**
     *
     * @param uri       Existing file uri to the file
     * @return File.Format. UNKNOWN if can't detect any format.
     */
    public static File.Format detect(URI uri) {
        for (Map.Entry<File.Format, Pattern> entry : FORMAT_MAP.entrySet()) {
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
            default:
                break;
        }

        //PLAIN
        return File.Format.UNKNOWN;
    }


}

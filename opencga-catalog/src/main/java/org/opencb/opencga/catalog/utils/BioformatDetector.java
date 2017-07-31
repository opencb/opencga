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

import org.apache.commons.lang3.math.NumberUtils;
import org.opencb.opencga.catalog.models.File;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by ralonso on 12/03/15.
 */
public class BioformatDetector {

//    protected static final Map<File.Bioformat, Pattern> bioformatMap = new HashMap<>();

//    static {
//        bioformatMap.put(File.Bioformat.ALIGNMENT, Pattern.compile(".*\\.(bam|sam|cram)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));
//        bioformatMap.put(File.Bioformat.VARIANT, Pattern.compile(".*\\.(vcf)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));
//        bioformatMap.put(File.Bioformat.PEDIGREE, Pattern.compile(".*\\.(ped)(\\.[\\w]+)*", Pattern.CASE_INSENSITIVE));
//    }

    public static File.Bioformat detect(URI uri) {
        return detect(uri, FormatDetector.detect(uri), CompressionDetector.detect(uri));
    }

    public static File.Bioformat detect(URI uri, File.Format format, File.Compression compression) {
        String path = uri.getPath();
        Path source = Paths.get(uri);
        String mimeType;

        try {
            switch (format) {
                case VCF:
                case GVCF:
                case BCF:
                    return File.Bioformat.VARIANT;
                case TBI:
                    break;
                case SAM:
                case BAM:
                case CRAM:
                    return File.Bioformat.ALIGNMENT;
                case BAI:
                    return File.Bioformat.NONE; //TODO: Alignment?
                case FASTQ:
                    return File.Bioformat.SEQUENCE;
                case PED:
                    return File.Bioformat.PEDIGREE;
                case TAB_SEPARATED_VALUES:
                    break;
                case COMMA_SEPARATED_VALUES:
                    break;
                case PROTOCOL_BUFFER:
                    break;
                case PLAIN:
                    break;
                case JSON:
                case AVRO:
                    String file;
                    if (compression != File.Compression.NONE) {
                        file = com.google.common.io.Files.getNameWithoutExtension(uri.getPath()); //Remove compression extension
                        file = com.google.common.io.Files.getNameWithoutExtension(file);  //Remove format extension
                    } else {
                        file = com.google.common.io.Files.getNameWithoutExtension(uri.getPath()); //Remove format extension
                    }

                    if (file.endsWith("variants")) {
                        return File.Bioformat.VARIANT;
                    } else if (file.endsWith("alignments")) {
                        return File.Bioformat.ALIGNMENT;
                    }
                    break;
                case PARQUET:
                    break;
                case IMAGE:
                case BINARY:
                case EXECUTABLE:
                case UNKNOWN:
                case XML:
                    return File.Bioformat.NONE;
                default:
                    break;
            }

//            for (Map.Entry<File.Bioformat, Pattern> entry : bioformatMap.entrySet()) {
//                if (entry.getValue().matcher(path).matches()) {
//                    return entry.getKey();
//                }
//            }

            mimeType = Files.probeContentType(source);

            if (path.endsWith(".nw")) {
                return File.Bioformat.OTHER_NEWICK;
            }

            if (mimeType == null
                    || !mimeType.equalsIgnoreCase("text/plain")
                    || path.endsWith(".redirection")
                    || path.endsWith(".Rout")
                    || path.endsWith("cel_files.txt")
                    || !path.endsWith(".txt")) {
                return File.Bioformat.NONE;
            }

            FileInputStream fstream = new FileInputStream(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

            String strLine;
            int numberOfLines = 20;

            int i = 0;
            boolean names = false;
            while ((strLine = br.readLine()) != null) {
                if (strLine.equalsIgnoreCase("")) {
                    continue;
                }
                if (i == numberOfLines) {
                    break;
                }
                if (strLine.startsWith("#")) {
                    if (strLine.startsWith("#NAMES")) {
                        names = true;
                    } else {
                        continue;
                    }
                } else {
                    String[] fields = strLine.split("\t");
                    if (fields.length > 2) {
                        if (names && NumberUtils.isNumber(fields[1])) {
                            return File.Bioformat.DATAMATRIX_EXPRESSION;
                        }
                    } else if (fields.length == 1) {
                        if (fields[0].split(" ").length == 1 && !NumberUtils.isNumber(fields[0])) {
                            return File.Bioformat.IDLIST;
                        }
                    } else if (fields.length == 2) {
                        if (!fields[0].contains(" ") && NumberUtils.isNumber(fields[1])) {
                            return File.Bioformat.IDLIST_RANKED;
                        }
                    }
                }
                i++;
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return File.Bioformat.NONE;
    }

}

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

package org.opencb.opencga.storage.core.variant.io;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsPopulationFrequencyExporter;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsTsvExporter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroWriter;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_STUDY;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF_GZ;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantWriterFactory {

    private static Logger logger = LoggerFactory.getLogger(VariantWriterFactory.class);
    private final StudyConfigurationManager studyConfigurationManager;

    public VariantWriterFactory(VariantDBAdaptor dbAdaptor) {
        studyConfigurationManager = dbAdaptor.getStudyConfigurationManager();
    }

    public VariantWriterFactory(StudyConfigurationManager studyConfigurationManager) {
        this.studyConfigurationManager = studyConfigurationManager;
    }

    public enum VariantOutputFormat {
        VCF("vcf", false),
        VCF_GZ("vcf.gz", false),
        JSON("json"),
        JSON_GZ("json.gz"),
        AVRO("avro"),
        AVRO_GZ("avro.gz"),
        AVRO_SNAPPY("avro.snappy"),
        PARQUET("parquet"),
        PARQUET_GZ("parquet.gz"),
        STATS("stats.tsv", false),
        STATS_GZ("stats.tsv.gz", false),
        CELLBASE("frequencies.json"),
        CELLBASE_GZ("frequencies.json.gz");

        private final boolean multiStudy;
        private final String extension;

        VariantOutputFormat(String extension) {
            this.extension = extension;
            this.multiStudy = true;
        }

        VariantOutputFormat(String extension, boolean multiStudy) {
            this.multiStudy = multiStudy;
            this.extension = extension;
        }

        public String getExtension() {
            return extension;
        }

        public boolean isMultiStudyOutput() {
            return multiStudy;
        }

        public boolean isGzip() {
            return extension.endsWith(".gz");
        }

        public boolean isSnappy() {
            return extension.endsWith(".snappy");
        }

    }

    /**
     * Transform the string to a valid output format.
     * If none, VCF by default.
     *
     * @param outputFormatStr   Output format as String
     * @param output            Output file
     * @return                  Valid VariantOutputFormat
     * @throws                  IllegalArgumentException if the outputFormatStr is not valid
     */
    public static VariantOutputFormat toOutputFormat(String outputFormatStr, String output) {
        if (!StringUtils.isEmpty(outputFormatStr)) {
            outputFormatStr = outputFormatStr.replace('.', '_');
            return VariantOutputFormat.valueOf(outputFormatStr.toUpperCase());
        } else if (isStandardOutput(output)) {
            return VCF;
        } else {
            return VCF_GZ;
        }
    }

    public static String checkOutput(@Nullable String output, VariantOutputFormat outputFormat) throws IOException {
        if (isStandardOutput(output)) {
            // Standard output
            return null;
        }
        if (output.endsWith("/")) {
            throw new IllegalArgumentException("Invalid directory as output file name");
        }
        if (output.endsWith(".")) {
            output = output.substring(0, output.length() - 1);
        }
        if (!output.endsWith(outputFormat.getExtension())) {
            String[] split = outputFormat.getExtension().split("\\.");
            int idx = 0;
            for (int i = 0; i < split.length; i++) {
                String s = split[i];
                if (output.endsWith('.' + s)) {
                    idx = i + 1;
                }
            }
            for (int i = idx; i < split.length; i++) {
                String s = split[i];
                if (!output.endsWith(s)) {
                    output = output + '.' + s;
                }
            }
        }

        Path path = Paths.get(output).toAbsolutePath();
        File file = path.toFile();
        if (file.isDirectory()) {
            throw new IOException(path + ": Is a directory");
        } else {
            if (file.exists()) {
                if (!file.canWrite()) {
                    throw new IOException(path + ": Permission denied");
                }
            } else {
                File parentFile = file.getParentFile();
                if (!parentFile.exists()) {
                    throw new IOException(parentFile + ": Not found");
                } else if (!parentFile.canWrite()) {
                    throw new IOException(parentFile + ": Permission denied");
                }
            }
        }
        return output;
    }

    public static OutputStream getOutputStream(String output, String outputFormatStr) throws IOException {
        VariantOutputFormat outputFormat = toOutputFormat(outputFormatStr, output);
        return getOutputStream(output, outputFormat);
    }

    public static OutputStream getOutputStream(String output, VariantOutputFormat outputFormat) throws IOException {
        boolean gzip = outputFormat.isGzip();

        // output format has priority over output name
        OutputStream outputStream;
        if (isStandardOutput(output)) {
            // Unclosable OutputStream
            outputStream = new UnclosableOutputStream(System.out);
        } else {
            outputStream = new FileOutputStream(output);
            logger.debug("writing to %s", output);
        }

        // If compressed a GZip output stream is used
        if (gzip && outputFormat != VariantOutputFormat.AVRO_GZ) {
            outputStream = new GZIPOutputStream(outputStream);
        } else {
            outputStream = new BufferedOutputStream(outputStream);
        }

        logger.debug("using {} output stream", gzip ? "gzipped" : "plain");

        return outputStream;
    }

    public DataWriter<Variant> newDataWriter(VariantOutputFormat outputFormat, final OutputStream outputStream,
                                                Query query, QueryOptions queryOptions) throws IOException {
        final DataWriter<Variant> exporter;

        switch (outputFormat) {
            case VCF_GZ:
            case VCF:
                VariantMetadataFactory metadataFactory = new VariantMetadataFactory(studyConfigurationManager);
                VariantMetadata variantMetadata;
                try {
                    variantMetadata = metadataFactory.makeVariantMetadata(query, queryOptions);
                } catch (StorageEngineException e) {
                    throw new IOException(e);
                }
                if (!variantMetadata.getStudies().isEmpty()) {
                    List<String> annotations = queryOptions.getAsStringList("annotations");
                    exporter = VcfDataWriter.newWriterForAvro(variantMetadata, annotations, outputStream);
                } else {
                    throw new IllegalArgumentException("No study found named " + query.getAsStringList(INCLUDE_STUDY.key()));
                }
                break;

            case JSON_GZ:
            case JSON:
                exporter = new VariantJsonWriter(outputStream);
                break;

            case AVRO:
            case AVRO_GZ:
            case AVRO_SNAPPY:
                String codecName = "";
                if (outputFormat.isGzip()) {
                    codecName = "gzip";
                } else if (outputFormat.isSnappy()) {
                    codecName = "snappy";
                }
                exporter = new VariantAvroWriter(VariantAvro.getClassSchema(), codecName, outputStream);
                break;

            case STATS_GZ:
            case STATS:
                StudyConfiguration sc = getStudyConfiguration(query, true);
                List<String> cohorts = new ArrayList<>(sc.getCohortIds().keySet());

                exporter = new VariantStatsTsvExporter(outputStream, sc.getStudyName(), cohorts);
                break;

            case CELLBASE_GZ:
            case CELLBASE:
                exporter = new VariantStatsPopulationFrequencyExporter(outputStream);
                break;

            default:
                throw variantFormatNotSupported(outputFormat.toString());
        }

        return exporter;
    }

    protected static IllegalArgumentException variantFormatNotSupported(String outputFormatStr) {
        return new IllegalArgumentException("Unknown output format " + outputFormatStr);
    }

    public static boolean isStandardOutput(String output) {
        return StringUtils.isEmpty(output);
    }

    protected StudyConfiguration getStudyConfiguration(Query query, boolean singleStudy) {
        List<Integer> studyIds = VariantQueryUtils.getIncludeStudies(query, QueryOptions.empty(), studyConfigurationManager);

        if (studyIds.isEmpty()) {
            studyIds = studyConfigurationManager.getStudyIds(null);
            if (studyIds == null) {
                throw new IllegalArgumentException();
            }
        }
        if (singleStudy) {
            if (studyIds.size() > 1) {
                throw new IllegalArgumentException();
            }
        }
        return studyConfigurationManager.getStudyConfiguration(studyIds.get(0), null).first();
    }

    /**
     * Unclosable output stream.
     *
     * Avoid passing System.out directly to HTSJDK, because it will close it at the end.
     *
     * http://stackoverflow.com/questions/8941298/system-out-closed-can-i-reopen-it/23791138#23791138
     */
    public static class UnclosableOutputStream extends FilterOutputStream {

        public UnclosableOutputStream(OutputStream os) {
            super(os);
        }

        @Override
        public void close() throws IOException {
            super.flush();
        }
    }
}

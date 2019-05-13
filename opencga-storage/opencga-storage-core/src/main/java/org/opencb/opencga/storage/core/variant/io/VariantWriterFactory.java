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
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOManagerProvider;
import org.opencb.opencga.storage.core.metadata.VariantMetadataFactory;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroWriter;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedList;
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
    private final VariantStorageMetadataManager variantStorageMetadataManager;

    public VariantWriterFactory(VariantDBAdaptor dbAdaptor) {
        variantStorageMetadataManager = dbAdaptor.getMetadataManager();
    }

    public VariantWriterFactory(VariantStorageMetadataManager variantStorageMetadataManager) {
        this.variantStorageMetadataManager = variantStorageMetadataManager;
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
        return toOutputFormat(outputFormatStr, StringUtils.isEmpty(output) ? null : URI.create(output));
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
    public static VariantOutputFormat toOutputFormat(String outputFormatStr, URI output) {
        if (!StringUtils.isEmpty(outputFormatStr)) {
            outputFormatStr = outputFormatStr.replace('.', '_');
            return VariantOutputFormat.valueOf(outputFormatStr.toUpperCase());
        } else if (isStandardOutput(output)) {
            return VCF;
        } else {
            return VCF_GZ;
        }
    }

    public static URI checkOutput(@Nullable URI output, VariantOutputFormat outputFormat) throws IOException {
        if (isStandardOutput(output)) {
            return null;
        }

        return UriUtils.replacePath(output, checkOutput(output.getPath(), outputFormat));
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

        return output;
    }

    public static OutputStream getOutputStream(URI output, VariantOutputFormat outputFormat, IOManagerProvider ioManagerProvider)
            throws IOException {
        boolean gzip = outputFormat.isGzip();

        // output format has priority over output name
        OutputStream outputStream;
        if (isStandardOutput(output)) {
            // Unclosable OutputStream
            outputStream = new UnclosableOutputStream(System.out);
        } else {
            outputStream = ioManagerProvider.newOutputStreamRaw(output);
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
                VariantMetadataFactory metadataFactory = new VariantMetadataFactory(variantStorageMetadataManager);
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
                StudyMetadata sm = getStudyMetadata(query, true);
                List<String> cohorts = new LinkedList<>();
                for (CohortMetadata cohort : variantStorageMetadataManager.getCalculatedCohorts(sm.getId())) {
                    cohorts.add(cohort.getName());
                }
                exporter = new VariantStatsTsvExporter(outputStream, sm.getName(), cohorts);
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

    public static boolean isStandardOutput(URI output) {
        return output == null;
    }

    protected StudyMetadata getStudyMetadata(Query query, boolean singleStudy) {
        List<Integer> studyIds = VariantQueryUtils.getIncludeStudies(query, QueryOptions.empty(), variantStorageMetadataManager);

        if (studyIds.isEmpty()) {
            studyIds = variantStorageMetadataManager.getStudyIds(null);
            if (studyIds == null) {
                throw new IllegalArgumentException();
            }
        }
        if (singleStudy) {
            if (studyIds.size() > 1) {
                throw new IllegalArgumentException();
            }
        }
        return variantStorageMetadataManager.getStudyMetadata(studyIds.get(0));
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

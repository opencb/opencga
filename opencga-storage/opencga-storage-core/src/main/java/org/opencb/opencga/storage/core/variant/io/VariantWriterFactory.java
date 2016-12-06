package org.opencb.opencga.storage.core.variant.io;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAvro;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsPopulationFrequencyExporter;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsTsvExporter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
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

import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams.RETURNED_STUDIES;

/**
 * Created on 06/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantWriterFactory {

    private static Logger logger = LoggerFactory.getLogger(VariantWriterFactory.class);
    private final VariantDBAdaptor dbAdaptor;

    public VariantWriterFactory(VariantDBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
    }

    public enum VariantOutputFormat {
        VCF(false),
        JSON,
        AVRO,
        STATS(false),
        CELLBASE;
//        VCF(false),
//        VCF_GZ(false),
//        JSON,
//        JSON_GZ,
//        AVRO,
//        AVRO_GZ,
//        AVRO_SNAPPY,
//        STATS(false),
//        STATS_GZ(false),
//        CELLBASE,
//        CELLBASE_GZ;

        private final boolean multiStudy;

        VariantOutputFormat() {
            this.multiStudy = true;
        }

        VariantOutputFormat(boolean multiStudy) {
            this.multiStudy = multiStudy;
        }

        public boolean isMultiStudyOutput() {
            return multiStudy;
        }

        static boolean isGzip(String value) {
            return value.endsWith(".gz");
        }

        static boolean isSnappy(String value) {
            return value.endsWith(".snappy");
        }

        static VariantOutputFormat safeValueOf(String value) {
            int index = value.indexOf(".");
            if (index >= 0) {
                value = value.substring(0, index);
            }
            value = value.toUpperCase();
//            if (isGzip(value)) {
//                value += "_GZ";
//            } else if (isSnappy(value)) {
//                value += "_SNAPPY";
//            }
            try {
                return VariantOutputFormat.valueOf(value);
            } catch (IllegalArgumentException ignore) {
                return null;
            }
        }

    }

    public static String checkOutput(@Nullable String output, String outputFormatStr) throws IOException {
        if (isStandardOutput(output)) {
            // Standard output
            return null;
        }
        if (VariantOutputFormat.isGzip(outputFormatStr)) {
            if (!output.toLowerCase().endsWith(".gz")) {
                output += ".gz";
            }
        }
        Path path = Paths.get(output);
        File file = path.toFile();
        if (file.isDirectory()) {
            throw new IOException("{}: Is a directory");
        } else {
            if (file.canWrite()) {
                throw new IOException("{}: Permission denied");
            }
        }
        return output;
    }

    public static OutputStream getOutputStream(String output, String outputFormatStr) throws IOException {
        /*
         * Output parameters
         */
        boolean gzip = true;
        VariantOutputFormat outputFormat;
        if (StringUtils.isNotEmpty(outputFormatStr)) {
            outputFormat = VariantOutputFormat.safeValueOf(outputFormatStr);
            if (outputFormat == null) {
                throw variantFormatNotSupported(outputFormatStr);
            } else {
                gzip = VariantOutputFormat.isGzip(outputFormatStr);
            }
        } else {
            outputFormat = VariantOutputFormat.VCF;
        }

        // output format has priority over output name
        OutputStream outputStream;
        if (isStandardOutput(output)) {
            // Unclosable OutputStream
            outputStream = new VariantVcfExporter.UnclosableOutputStream(System.out);
        } else {
            outputStream = new FileOutputStream(output);
            logger.debug("writing to %s", output);
        }

        // If compressed a GZip output stream is used
        if (gzip && outputFormat != VariantOutputFormat.AVRO) {
            outputStream = new GZIPOutputStream(outputStream);
        } else {
            outputStream = new BufferedOutputStream(outputStream);
        }

        logger.debug("using %s output stream", gzip ? "gzipped" : "plain");

        return outputStream;
    }

    public DataWriter<Variant> newDataWriter(VariantOutputFormat outputFormat, OutputStream outputStream, Query query,
                                             QueryOptions queryOptions) {
        return newDataWriter(outputFormat, outputFormat.toString(), outputStream, query, queryOptions);
    }

    public DataWriter<Variant> newDataWriter(String outputFormat, OutputStream outputStream, Query query, QueryOptions queryOptions) {
        return newDataWriter(VariantOutputFormat.safeValueOf(outputFormat), outputFormat, outputStream, query, queryOptions);
    }

    protected DataWriter<Variant> newDataWriter(VariantOutputFormat outputFormat, String outputFormatStr, OutputStream outputStream,
                                                Query query, QueryOptions queryOptions) {
        final DataWriter<Variant> exporter;

        switch (outputFormat) {
            case VCF:
                StudyConfiguration studyConfiguration = getStudyConfiguration(query, dbAdaptor, true);
                if (studyConfiguration != null) {
                    // Samples to be returned
                    if (query.containsKey(RETURNED_SAMPLES.key())) {
                        queryOptions.put(RETURNED_SAMPLES.key(), query.get(RETURNED_SAMPLES.key()));
                    }

                    // TODO:
//                    if (cliOptions.annotations != null) {
//                        queryOptions.add("annotations", cliOptions.annotations);
//                    }

                    VariantSourceDBAdaptor sourceDBAdaptor = dbAdaptor.getVariantSourceDBAdaptor();
                    exporter = new VariantVcfExporter(studyConfiguration, sourceDBAdaptor, outputStream, queryOptions);
                } else {
                    throw new IllegalArgumentException("No study found named " + query.getAsStringList(RETURNED_STUDIES.key()).get(0));
                }
                break;
            case JSON:
//                exporter = batch -> {
//                    batch.forEach(variant -> {
//                        try {
//                            outputStream.write(variant.toJson().getBytes());
//                            outputStream.write('\n');
//                        } catch (IOException e) {
//                            throw new UncheckedIOException(e);
//                        }
//                    });
//                    return true;
//                };
                exporter = new VariantJsonWriter(outputStream);

                break;
            case AVRO:
                String codecName = "";
                if (VariantOutputFormat.isGzip(outputFormatStr)) {
                    codecName = "gzip";
                } else {
                    if (VariantOutputFormat.isSnappy(outputFormatStr)) {
                        codecName = "snappy";
                    }
                }
                exporter = new VariantAvroWriter(VariantAvro.getClassSchema(), codecName, outputStream);

                break;
            case STATS:
                StudyConfiguration sc = getStudyConfiguration(query, dbAdaptor, true);
                List<String> cohorts = new ArrayList<>(sc.getCohortIds().keySet());

                exporter = new VariantStatsTsvExporter(outputStream, sc.getStudyName(), cohorts);

                break;
            case CELLBASE:
                exporter = new VariantStatsPopulationFrequencyExporter(outputStream);
                break;
            default:
                throw variantFormatNotSupported(outputFormatStr);
        }

        return exporter;
    }

    protected static IllegalArgumentException variantFormatNotSupported(String outputFormatStr) {
        return new IllegalArgumentException("Unknown output format " + outputFormatStr);
    }

    public static boolean isStandardOutput(String output) {
        return StringUtils.isEmpty(output);
    }

    protected StudyConfiguration getStudyConfiguration(Query query, VariantDBAdaptor dbAdaptor, boolean singleStudy) {
        List<Integer> studyIds = dbAdaptor.getReturnedStudies(query, QueryOptions.empty());

        StudyConfigurationManager scm = dbAdaptor.getStudyConfigurationManager();
        if (studyIds.isEmpty()) {
            studyIds = scm.getStudyIds(null);
            if (studyIds == null) {
                throw new IllegalArgumentException();
            }
        }
        if (singleStudy) {
            if (studyIds.size() > 1) {
                throw new IllegalArgumentException();
            }
        }
        return scm.getStudyConfiguration(studyIds.get(0), null).first();
    }

}

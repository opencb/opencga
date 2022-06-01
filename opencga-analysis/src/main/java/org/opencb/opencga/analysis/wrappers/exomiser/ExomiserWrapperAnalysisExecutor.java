package org.opencb.opencga.analysis.wrappers.exomiser;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPOutputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;

@ToolExecutor(id = ExomiserWrapperAnalysisExecutor.ID,
        tool = ExomiserWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ExomiserWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor implements StorageToolExecutor  {

    public final static String ID = ExomiserWrapperAnalysis.ID + "-local";

    private String study;
    private String sample;

    private VariantDBAdaptor dbAdaptor;
    private Logger logger = LoggerFactory.getLogger(this.getClass());


    @Override
    public void run() throws ToolException {

        Path vcfPath = getOutDir().resolve(sample + ".vcf.gz");
        Query query = new Query(SAMPLE.key(), sample);
        try {
            export(vcfPath, query, QueryOptions.empty());
        } catch (IOException e) {
            throw new ToolException(e);
        }

        StringBuilder sb = initCommandLine();

//        // Append mounts
//        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getFastaFile(),
//                null, getExecutorParams());
//        Map<String, String> mountMap = appendMounts(inputFilenames, sb);
//
//        // Append docker image, version and command
//        appendCommand("bwa " + command, sb);
//
//        // Append other params
//        appendOtherParams(null, sb);
//
//        // Append input file params
//        appendInputFiles(inputFilenames, mountMap, sb);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb.toString());
        runCommandLine(sb.toString());
    }

    private int export(Path outputVcf, Query query, QueryOptions options) throws IOException {
        try (GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(outputVcf.toFile()))) {
            DataWriter<Variant> writer = new VariantWriterFactory(dbAdaptor).newDataWriter(VariantWriterFactory.VariantOutputFormat.VCF_GZ,
                    outputStream, query, options);

            writer.open();
            writer.pre();
            dbAdaptor.iterator(query, options).forEachRemaining(writer::write);
            writer.post();
            writer.close();
        }
        return 0;
    }


    public String getStudy() {
        return study;
    }

    public ExomiserWrapperAnalysisExecutor setStudy(String study) {
        this.study = study;
        return this;
    }

    public ExomiserWrapperAnalysisExecutor setSample(String sample) {
        this.sample = sample;
        return this;
    }
}

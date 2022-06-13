package org.opencb.opencga.analysis.wrappers.exomiser;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
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
public class ExomiserWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor implements StorageToolExecutor {

    public final static String ID = ExomiserWrapperAnalysis.ID + "-local";

    public final static String DOCKER_IMAGE_NAME = "exomiser/exomiser-cli";
    public final static String DOCKER_IMAGE_VERSION = "";

    private String studyId;
    private String sampleId;

    private VariantDBAdaptor dbAdaptor;
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException {
        Path vcfPath = getOutDir().resolve(sampleId + ".vcf.gz");
        Query query = new Query(SAMPLE.key(), sampleId);
        try {
            getVariantStorageManager().exportData(vcfPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, query,
                    QueryOptions.empty(), getToken());
        } catch (StorageEngineException | CatalogException e) {
            throw new ToolException(e);
        }

        List<String> hpos = new ArrayList<>();
        Individual individual = IndividualQcUtils.getIndividualBySampleId(studyId, sampleId, getVariantStorageManager().getCatalogManager(),
                getToken());

        if (CollectionUtils.isNotEmpty(individual.getPhenotypes())) {
            for (Phenotype phenotype : individual.getPhenotypes()) {
                if (phenotype.getId().startsWith("HPO")) {
                    hpos.add(phenotype.getId());
                }
            }
        }

        // Check HPOs
        if (CollectionUtils.isEmpty(hpos)) {
            throw new ToolException("Missing phenotype, i.e. HPO terms, for individual/sample (" + individual.getId() + "/" + sampleId
                    + ")");
        }


        StringBuilder sb = initCommandLine();

//        // Append mounts
//        List<Pair<String, String>> inputFilenames = DockerWrapperAnalysisExecutor.getInputFilenames(getFastaFile(),
//                null, getExecutorParams());
//        Map<String, String> mountMap = appendMounts(inputFilenames, sb);
//
        // Append docker image, version and command
        appendCommand("", sb);
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

    private String getOpencgaHome() throws ToolExecutorException {
        String opencgaHome = getExecutorParams().getString("opencgaHome");
        if (StringUtils.isEmpty(opencgaHome)) {
            throw new ToolExecutorException("Missing OpenCGA home in executor params!");
        }
        return opencgaHome;
    }

    @Override
    public String getDockerImageName() {
        return DOCKER_IMAGE_NAME;
    }

    @Override
    public String getDockerImageVersion() {
        return DOCKER_IMAGE_VERSION;
    }

    public String getStudyId() {
        return studyId;
    }

    public ExomiserWrapperAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    public String getSampleId() {
        return sampleId;
    }

    public ExomiserWrapperAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }
}

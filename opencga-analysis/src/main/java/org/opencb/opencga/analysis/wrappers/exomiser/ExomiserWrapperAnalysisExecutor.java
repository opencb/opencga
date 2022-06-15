package org.opencb.opencga.analysis.wrappers.exomiser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;

@ToolExecutor(id = ExomiserWrapperAnalysisExecutor.ID,
        tool = ExomiserWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ExomiserWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor implements StorageToolExecutor {

    public final static String ID = ExomiserWrapperAnalysis.ID + "-local";

    private final static String EXOMISER_ANALYSIS_TEMPLATE_FILENAME = "exomiser-analysis.yml";
    private final static String EXOMISER_PROPERTIES_TEMPLATE_FILENAME = "application.properties";

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

        // Check HPOs
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

        if (CollectionUtils.isEmpty(hpos)) {
            throw new ToolException("Missing phenotype, i.e. HPO terms, for individual/sample (" + individual.getId() + "/" + sampleId
                    + ")");
        }

        Path openCgaHome = getOpencgaHomePath();
        Path exomiserDataPath = getExomiserDataPath(openCgaHome);

        // Get exomiser analysis file and update with the individual HPOs
        String analysisTemplateContent;
        try {
            analysisTemplateContent = getExomiserAnalysisTemplate(openCgaHome);
        } catch (IOException e) {
            throw new ToolException("Error reading Exomiser analysis template file", e);
        }
        StringBuilder hpoSb = new StringBuilder();
        for (String hpo : hpos) {
            if (hpoSb.length() > 0) {
                hpoSb.append(", ");
            }
            hpoSb.append("'" + hpo + "'");
        }
        analysisTemplateContent.replace("___HPOS___", hpoSb.toString());
        try {
            FileUtils.write(getOutDir().resolve(EXOMISER_ANALYSIS_TEMPLATE_FILENAME).toFile(), analysisTemplateContent,
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ToolException("Error writing Exomiser analysis file", e);
        }

        // And copy the application.properties
        try {
            FileUtils.copyFile(openCgaHome.resolve("/analysis/exomiser/" + EXOMISER_PROPERTIES_TEMPLATE_FILENAME).toFile(),
                    getOutDir().resolve(EXOMISER_PROPERTIES_TEMPLATE_FILENAME).toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser properties file", e);
        }

        // Build the docker command line to run Exomiser
        StringBuilder sb = initCommandLine();

        // Append mounts
        sb.append(" --mount type=bind,source=" + exomiserDataPath + ",target=/input")
                .append(" --mount type=bind,source=" + getOutDir() + ",target=/outdir");

        // Append docker image, version and command
        appendCommand("", sb);

        // Append input file params
        sb.append(" --analysis /outdir/").append(EXOMISER_ANALYSIS_TEMPLATE_FILENAME)
                .append(" --vcf /outdir/" + vcfPath)
                .append(" --spring.config.location=/outdir/").append(EXOMISER_PROPERTIES_TEMPLATE_FILENAME);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: " + sb);
        runCommandLine(sb.toString());
    }

    private Path getOpencgaHomePath() throws ToolExecutorException {
        String value = getExecutorParams().getString("opencgaHome");
        if (StringUtils.isEmpty(value)) {
            throw new ToolExecutorException("Missing OpenCGA home in executor parameters.");
        }

        Path path = Paths.get(value);
        if (!path.toFile().exists()) {
            throw new ToolExecutorException("OpenCGA home path (" + value +  ") does not exist.");
        }
        return path;
    }

    private Path getExomiserDataPath(Path openCgaHome) throws ToolException {
        Path path = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Map<String, String> map = objectMapper.readValue(openCgaHome.resolve("/analysis/exomiser/opencga-config.yaml").toFile(),
                    Map.class);
            path = Paths.get(map.get("data"));
        } catch (IOException e) {
            throw new ToolException("Error reading Exomiser configuration file", e);
        }

        if (!path.toFile().exists()) {
            throw new ToolExecutorException("Exomiser data path (" + path +  ") does not exist.");
        }
        return path;
    }

    private String getExomiserAnalysisTemplate(Path openCgaHome) throws IOException {
        Path path = openCgaHome.resolve("/analysis/exomiser/" + EXOMISER_ANALYSIS_TEMPLATE_FILENAME);
        return FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8);
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

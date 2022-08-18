package org.opencb.opencga.analysis.wrappers.exomiser;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;

@ToolExecutor(id = ExomiserWrapperAnalysisExecutor.ID,
        tool = ExomiserWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ExomiserWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor implements StorageToolExecutor {

    public final static String ID = ExomiserWrapperAnalysis.ID + "-local";

    private final static String EXOMISER_ANALYSIS_TEMPLATE_FILENAME = "exomiser-analysis.yml";
    private final static String EXOMISER_PROPERTIES_TEMPLATE_FILENAME = "application.properties";
    private static final String EXOMISER_OUTPUT_OPTIONS_FILENAME = "output.yml";

    public final static String DOCKER_IMAGE_NAME = "exomiser/exomiser-cli";
    public final static String DOCKER_IMAGE_VERSION = "";

    private String studyId;
    private String sampleId;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException {
        // Check HPOs, it will use a set to avoid duplicate HPOs,
        // and it will check both phenotypes and disorders
        logger.info("{}: Checking individual for sample {} in study {}", ID, sampleId, studyId);
        Set<String> hpos = new HashSet<>();
        Individual individual = IndividualQcUtils.getIndividualBySampleId(studyId, sampleId, getVariantStorageManager().getCatalogManager(),
                getToken());
        logger.info("{}: Individual found: {}", ID, individual.getId());
        if (CollectionUtils.isNotEmpty(individual.getPhenotypes())) {
            for (Phenotype phenotype : individual.getPhenotypes()) {
                if (phenotype.getId().startsWith("HP:")) {
                    hpos.add(phenotype.getId());
                }
            }
        }
        if (CollectionUtils.isNotEmpty(individual.getDisorders())) {
            for (Disorder disorder : individual.getDisorders()) {
                if (disorder.getId().startsWith("HP:")) {
                    hpos.add(disorder.getId());
                }
            }
        }

        if (CollectionUtils.isEmpty(hpos)) {
            throw new ToolException("Missing phenotypes, i.e. HPO terms, for individual/sample (" + individual.getId() + "/" + sampleId
                    + ")");
        }

        logger.info("{}: Getting HPO for individual {}: {}", ID, individual.getId(), StringUtils.join(hpos, ","));
        try {
            PrintWriter pw = new PrintWriter(getOutDir().resolve(sampleId + ".yml").toAbsolutePath().toString());
            pw.write("# a v1 phenopacket describing an individual https://phenopacket-schema.readthedocs.io/en/1.0.0/phenopacket.html\n");
            pw.write("---\n");
            pw.write("subject:\n");
            pw.write("    id: " + sampleId + "\n");
            pw.write("phenotypicFeatures:\n");
            for (String hpo : hpos) {
                pw.write("    - type:\n");
                pw.write("        id: " + hpo + "\n");
            }
            pw.close();
        } catch (IOException e) {
            throw new ToolException("Error writing Exomiser sample file", e);
        }

        // Export data into VCF file
        Path vcfPath = getOutDir().resolve(sampleId + ".vcf.gz");
        VariantQuery query = new VariantQuery()
                .study(studyId)
                .sample(sampleId)
                .includeSampleId(true)
                .includeGenotype(true);
        logger.info("{}: Exomiser exports variants using the query: {}", ID, query.toJson());
        try {
            getVariantStorageManager().exportData(vcfPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, query,
                    QueryOptions.empty(), getToken());
        } catch (StorageEngineException | CatalogException e) {
            throw new ToolException(e);
        }

        // Get Exomiser (external) data
        Path openCgaHome = getOpencgaHomePath();
        Path exomiserDataPath = getAnalysisDataPath(ExomiserWrapperAnalysis.ID);

        // And copy the application.properties
        try {
            FileUtils.copyFile(openCgaHome.resolve("analysis/exomiser/" + EXOMISER_PROPERTIES_TEMPLATE_FILENAME).toFile(),
                    getOutDir().resolve(EXOMISER_PROPERTIES_TEMPLATE_FILENAME).toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser properties file", e);
        }

        // And copy the output options
        try {
            FileUtils.copyFile(openCgaHome.resolve("analysis/exomiser/" + EXOMISER_OUTPUT_OPTIONS_FILENAME).toFile(),
                    getOutDir().resolve(EXOMISER_OUTPUT_OPTIONS_FILENAME).toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser output options file", e);
        }

        // Build the docker command line to run Exomiser
        StringBuilder sb = initCommandLine();

        // Append mounts
        sb.append(" --mount type=bind,source=" + exomiserDataPath + ",target=/data")
                .append(" --mount type=bind,source=" + getOutDir() + ",target=/jobdir ");

        // Append docker image, version and command
        appendCommand("", sb);

        // Append input file params
//        sb.append(" --analysis /jobdir/").append(EXOMISER_ANALYSIS_TEMPLATE_FILENAME)
        sb.append(" --sample /jobdir/").append(sampleId).append(".yml")
                .append(" --vcf /jobdir/" + vcfPath.getFileName())
                .append(" --assembly hg38 --output /jobdir/").append(EXOMISER_OUTPUT_OPTIONS_FILENAME)
                .append(" --spring.config.location=/jobdir/").append(EXOMISER_PROPERTIES_TEMPLATE_FILENAME);

        // Execute command and redirect stdout and stderr to the files
        logger.info("{}: Docker command line: {}", ID, sb);
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

    private Path getAnalysisDataPath(String analysisId) throws ToolException {
        if (ExomiserWrapperAnalysis.ID.equals(analysisId)) {
            return getExomiserDataPath(getOpencgaHomePath());
        }
        return getOpencgaHomePath().resolve("analysis/resources");
    }

    private Path getExomiserDataPath(Path openCgaHome) throws ToolException {
        Path exomiserDataPath = openCgaHome.resolve("analysis/resources/exomiser");
        if (!exomiserDataPath.toFile().exists()) {
            if (!exomiserDataPath.toFile().mkdirs()) {
                throw new ToolException("Error creating the Exomiser data directory");
            }
        }

        // Mutex management to avoid multiple downloadings at the same time
        // the first to come, download data, others have to wait for
        File readyFile = exomiserDataPath.resolve("READY").toFile();
        File preparingFile = exomiserDataPath.resolve("PREPARING").toFile();

        // If all is ready, then return
        if (readyFile.exists()) {
            logger.info("{}: Exomiser data is already downloaded, so Exomiser analysis is ready to be executed.", ID);
            return exomiserDataPath;
        }

        // If it is preparing, then wait for ready and then return
        if (preparingFile.exists()) {
            long startTime = System.currentTimeMillis();
            logger.info("{}: Exomiser data is downloading, waiting for it...", ID);
            while (!readyFile.exists()) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // Nothing to do here
                    throw new ToolException(e);
                }
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime > 18000000) {
                    throw new ToolException("Unable to run the Exomiser analysis because of Exomiser data is not ready yet: maximum"
                            + " waiting time exceeded");
                }
            }
            logger.info("{}: Exomiser data is now downloaded: Exomiser analysis is ready to be executed", ID);
            return exomiserDataPath;
        }

        // Mark as preparing
        try {
            preparingFile.createNewFile();
        } catch (IOException e) {
            throw new ToolException("Error creating the Exomiser data directory");
        }

        // Download and unzip files
        try {
            downloadAndUnzip(exomiserDataPath, "2109_hg38.zip");
            downloadAndUnzip(exomiserDataPath, "2109_phenotype.zip");
        } catch (ToolException e) {
            // If something wrong happened, the preparing file has to be deleted
            preparingFile.delete();
            throw new ToolException("Something wrong happened when preparing Exomiser data", e);
        }

        // Mutex management, signal exomiser data is ready
        try {
            readyFile.createNewFile();
        } catch (IOException e) {
            throw new ToolException("Error preparing Exomiser data", e);
        }
        preparingFile.delete();

        return exomiserDataPath;
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

    private void downloadAndUnzip(Path exomiserDataPath, String filename) throws ToolException {
        URL url = null;

        // Download data
        try {
            url = new URL("http://resources.opencb.org/opencb/opencga/analysis/exomiser/" + filename);
            logger.info("{}: Downloading Exomiser data: {}", ID, url);
            ResourceUtils.downloadThirdParty(url, exomiserDataPath);
        } catch (IOException e) {
            throw new ToolException("Error downloading Exomiser data from " + url, e);
        }

        // Unzip
        try {
            logger.info("{}: Decompressing Exomiser data: {}", ID, filename);
            new Command("unzip -o -d " + exomiserDataPath + " " + exomiserDataPath + "/" + filename)
                    .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stdout_unzip_"
                            + filename + ".txt").toFile())))
                    .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stderr_unzip_"
                            + filename + ".txt").toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException("Error unzipping Exomiser data: " + filename, e);
        }

        // Free disk space
        logger.info("{}: Deleting Exomiser data: {}", ID, filename);
        exomiserDataPath.resolve(filename).toFile().delete();
    }

    public String getSampleId() {
        return sampleId;
    }

    public ExomiserWrapperAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }
}

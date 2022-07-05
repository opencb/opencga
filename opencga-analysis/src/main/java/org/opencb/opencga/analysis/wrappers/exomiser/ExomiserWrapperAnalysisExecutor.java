package org.opencb.opencga.analysis.wrappers.exomiser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
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
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.SAMPLE;

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
        Path vcfPath = getOutDir().resolve(sampleId + ".vcf.gz");
        Query query = new Query(SAMPLE.key(), sampleId);
        try {
            getVariantStorageManager().exportData(vcfPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, query,
                    QueryOptions.empty(), getToken());
        } catch (StorageEngineException | CatalogException e) {
            throw new ToolException(e);
        }

        // Check HPOs, it will use a set to avoid duplicate HPOs,
        // and it will check both phenotypes and disorders
        Set<String> hpos = new HashSet<>();
        Individual individual = IndividualQcUtils.getIndividualBySampleId(studyId, sampleId, getVariantStorageManager().getCatalogManager(),
                getToken());

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
            throw new ToolException("Missing phenotype, i.e. HPO terms, for individual/sample (" + individual.getId() + "/" + sampleId
                    + ")");
        }

        // Create Exomiser sample file from HPOs
        StringBuilder hpoSb = new StringBuilder();
        for (String hpo : hpos) {
            if (hpoSb.length() > 0) {
                hpoSb.append(", ");
            }
            hpoSb.append("'" + hpo + "'");
        }
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

        if (!readyFile.exists()) {
            if (preparingFile.exists()) {
                // wait for ready
                while (!readyFile.exists()) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        // Nothing to do here
                    }
                }
            } else {
                try {
                    if (!new File(preparingFile.getParent()).exists()) {
                        new File(preparingFile.getParent()).mkdirs();
                    }
                    preparingFile.createNewFile();
                } catch (IOException e) {
                    throw new ToolException("Error creating the Exomiser data directory");
                }
            }
        }

        String genomeFilename = "2109_hg38.zip";
        String phenotypeFilename = "2109_phenotype.zip";
        try {
            ResourceUtils.downloadThirdParty(new URL("http://resources.opencb.org/opencb/opencga/analysis/exomiser/" + genomeFilename),
                    exomiserDataPath);
        } catch (IOException e) {
            throw new ToolException("Error downloading Exomiser hg38 data", e);
        }

        try {
            ResourceUtils.downloadThirdParty(new URL("http://resources.opencb.org/opencb/opencga/analysis/exomiser/" + phenotypeFilename),
                    exomiserDataPath);
        } catch (IOException e) {
            throw new ToolException("Error downloading Exomiser phenotype data", e);
        }

        // Unzip
        try {
            new Command("unzip -o -d " + exomiserDataPath + " " + exomiserDataPath + "/" + genomeFilename)
                    .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stdout_unzip_"
                                    + genomeFilename + ".txt").toFile())))
                    .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stderr_unzip_"
                            + genomeFilename + ".txt").toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException("Error unzipping Exomiser genome data", e);
        }
        try {
            new Command("unzip -o -d " + exomiserDataPath + " " + exomiserDataPath + "/" + phenotypeFilename)
                    .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stdout_unzip_"
                            + phenotypeFilename + ".txt").toFile())))
                    .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stderr_unzip_"
                            + phenotypeFilename + ".txt").toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException("Error unzipping Exomiser phenotype data", e);
        }

        // Mutex management, signal exomiser data is ready
        if (preparingFile.exists()) {
            try {
                readyFile.createNewFile();
            } catch (IOException e) {
                throw new ToolException("Error preparing Exomiser data", e);
            }
            preparingFile.delete();
        }

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

    public String getSampleId() {
        return sampleId;
    }

    public ExomiserWrapperAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }
}

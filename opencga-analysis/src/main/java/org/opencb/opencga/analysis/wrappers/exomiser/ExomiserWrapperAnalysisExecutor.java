package org.opencb.opencga.analysis.wrappers.exomiser;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.exec.Command;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.StorageToolExecutor;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.commons.utils.FileUtils.copyFile;
import static org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysis.*;

@ToolExecutor(id = ExomiserWrapperAnalysisExecutor.ID,
        tool = ExomiserWrapperAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class ExomiserWrapperAnalysisExecutor extends DockerWrapperAnalysisExecutor implements StorageToolExecutor {

    public final static String ID = ExomiserWrapperAnalysis.ID + "-local";

    private final static String EXOMISER_ANALYSIS_TEMPLATE_FILENAME = "exomiser-analysis.yml";
    private final static String EXOMISER_PROPERTIES_TEMPLATE_FILENAME = "application.properties";
    private static final String EXOMISER_OUTPUT_OPTIONS_FILENAME = "output.yml";

    // These constants must match in the file application.properties to be replaced
    private static final String HG38_DATA_VERSION_MARK = "put_here_hg38_data_version";
    private static final String PHENOTYPE_DATA_VERSION_MARK = "put_here_phenotype_data_version";
    private static final String CLINVAR_WHITELIST_MARK = "put_here_clinvar_whitelist";

    private String studyId;
    private String sampleId;
    private String exomiserVersion;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void run() throws ToolException, IOException, CatalogException {
        // Check HPOs, it will use a set to avoid duplicate HPOs,
        // and it will check both phenotypes and disorders
        logger.info("Checking individual for sample {} in study {}", sampleId, studyId);
        Set<String> hpos = new HashSet<>();
        Individual individual = IndividualQcUtils.getIndividualBySampleId(studyId, sampleId, getVariantStorageManager().getCatalogManager(),
                getToken());

        // Check assembly
        String assembly = IndividualQcUtils.getAssembly(studyId, getVariantStorageManager().getCatalogManager(), getToken());
        if (assembly.equalsIgnoreCase("GRCh38")) {
            assembly = "hg38";
//        } else if (assembly.equalsIgnoreCase("GRCh37")) {
//            assembly = "hg19";
        } else {
            throw new ToolException("Invalid assembly '" + assembly + "'. Supported assemblies are: GRCh38");
        }

        // Set father and mother if necessary (family ?)
        if (individual.getFather() != null && StringUtils.isNotEmpty(individual.getFather().getId())) {
            individual.setFather(IndividualQcUtils.getIndividualById(studyId, individual.getFather().getId(),
                    getVariantStorageManager().getCatalogManager(), getToken()));
        }
        if (individual.getMother() != null && StringUtils.isNotEmpty(individual.getMother().getId())) {
            individual.setMother(IndividualQcUtils.getIndividualById(studyId, individual.getMother().getId(),
                    getVariantStorageManager().getCatalogManager(), getToken()));
        }

        logger.info("Individual found: {}", individual.getId());
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
        logger.info("Getting HPO for individual {}: {}", individual.getId(), StringUtils.join(hpos, ","));

        List<String> samples = new ArrayList<>();
        samples.add(sampleId);

        // Check multi-sample (family) analysis
        File pedigreeFile = null;
        Pedigree pedigree = null;
        if (individual.getMother() != null && individual.getMother().getId() != null
                && individual.getFather() != null && individual.getFather().getId() != null) {
            Family family = IndividualQcUtils.getFamilyByIndividualId(getStudyId(), individual.getId(),
                    getVariantStorageManager().getCatalogManager(), getToken());
            if (family != null) {
                pedigree = FamilyManager.getPedigreeFromFamily(family, individual.getId());
            }

            if (pedigree != null) {
                if (individual.getFather() != null) {
                    samples.add(individual.getFather().getSamples().get(0).getId());
                }
                if (individual.getMother() != null) {
                    samples.add(individual.getMother().getSamples().get(0).getId());
                }
                pedigreeFile = createPedigreeFile(family, pedigree);
            }
        }
        File sampleFile = createSampleFile(individual, hpos, pedigree);

        // Export data into VCF file
        Path vcfPath = getOutDir().resolve(sampleId + ".vcf.gz");

        VariantQuery query = new VariantQuery()
                .study(studyId)
                .sample(sampleId)
                .includeSample(samples)
                .includeSampleData("GT")
                .unknownGenotype("./.")
                .append("includeAllFromSampleIndex", true);

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,studies.samples");

        logger.info("Exomiser exports variants using the query: {}", query.toJson());
        logger.info("Exomiser exports variants using the query options: {}", queryOptions.toJson());

        try {
            getVariantStorageManager().exportData(vcfPath.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null, query,
                    queryOptions, getToken());
        } catch (StorageEngineException | CatalogException e) {
            throw new ToolException(e);
        }

        // Get Exomiser (external) data
        Path openCgaHome = getOpencgaHomePath();
        Path exomiserDataPath = getAnalysisDataPath(ExomiserWrapperAnalysis.ID);

        // Copy the analysis
        try {
            copyFile(openCgaHome.resolve("analysis/exomiser/" + EXOMISER_ANALYSIS_TEMPLATE_FILENAME).toFile(),
                    getOutDir().resolve(EXOMISER_ANALYSIS_TEMPLATE_FILENAME).toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser analysis file", e);
        }

        // Copy the application.properties and update data according to Exomiser version
        try {
            Path target = getOutDir().resolve(EXOMISER_PROPERTIES_TEMPLATE_FILENAME);
            copyFile(openCgaHome.resolve("analysis/exomiser/" + EXOMISER_PROPERTIES_TEMPLATE_FILENAME).toFile(), target.toFile());
            // Update hg38 data version
            Command cmd = new Command("sed -i \"s/" + HG38_DATA_VERSION_MARK + "/" + getHg38DataVersion() + "/g\" " + target);
            cmd.run();
            // Update phenotype data version
            cmd = new Command("sed -i \"s/" + PHENOTYPE_DATA_VERSION_MARK + "/" + getPhenotypeDataVersion() + "/g\" " + target);
            cmd.run();
            // Update clinvar whitelist
            String whitelist;
            String clinvarWhitelistFilename = getHg38DataVersion() + "_hg38_clinvar_whitelist.tsv.gz";
            if (Files.exists(exomiserDataPath.resolve(getHg38DataVersion() + "_" + assembly).resolve(clinvarWhitelistFilename))) {
                whitelist = "exomiser.hg38.variant-white-list-path=" + clinvarWhitelistFilename;
            } else {
                whitelist = "#exomiser.hg38.variant-white-list-path=${exomiser.hg38.data-version}_hg38_clinvar_whitelist.tsv.gz";
            }
            cmd = new Command("sed -i \"s/" + CLINVAR_WHITELIST_MARK + "/" + whitelist + "/g\" " + target);
            cmd.run();
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser properties file", e);
        }

        // Copy the output options
        try {
            copyFile(openCgaHome.resolve("analysis/exomiser/" + EXOMISER_OUTPUT_OPTIONS_FILENAME).toFile(),
                    getOutDir().resolve(EXOMISER_OUTPUT_OPTIONS_FILENAME).toFile());
        } catch (IOException e) {
            throw new ToolException("Error copying Exomiser output options file", e);
        }

        // Build the docker command line to run Exomiser
        String[] userAndGroup = FileUtils.getUserAndGroup(getOutDir(), true);
        String dockerUser = userAndGroup[0] + ":" + userAndGroup[1];
        logger.info("Docker user: {}", dockerUser);
        StringBuilder sb = initCommandLine(dockerUser);

        // Append mounts
        sb.append(" --mount type=bind,source=" + exomiserDataPath + ",target=/data")
                .append(" --mount type=bind,source=" + getOutDir() + ",target=/jobdir ");

        // Append docker image, version and command
        appendCommand("", sb);

        // Append input file params
        sb.append(" --analysis /jobdir/").append(EXOMISER_ANALYSIS_TEMPLATE_FILENAME);
        sb.append(" --sample /jobdir/").append(sampleFile.getName());
        if (pedigreeFile != null && pedigreeFile.exists()) {
            sb.append(" --ped /jobdir/").append(pedigreeFile.getName());
        }
        sb.append(" --vcf /jobdir/" + vcfPath.getFileName())
                .append(" --assembly ").append(assembly)
                .append(" --output /jobdir/").append(EXOMISER_OUTPUT_OPTIONS_FILENAME)
                .append(" --spring.config.location=/jobdir/").append(EXOMISER_PROPERTIES_TEMPLATE_FILENAME);

        // Execute command and redirect stdout and stderr to the files
        logger.info("Docker command line: {}", sb);
        runCommandLine(sb.toString());
    }

    private File createSampleFile(Individual individual, Set<String> hpos, Pedigree pedigree) throws ToolException {
        File sampleFile = getOutDir().resolve(sampleId + ".yml").toFile();
        try {
            PrintWriter pw = new PrintWriter(sampleFile);
            pw.write("# a v1 phenopacket describing an individual https://phenopacket-schema.readthedocs.io/en/1.0.0/phenopacket.html\n");
            pw.write("---\n");
            pw.write("id: " + sampleId + "\n");
            String prefix = "";
            if (pedigree != null) {
                prefix = "  ";
                pw.write("proband:\n");
            }
            pw.write(prefix + "subject:\n");
            pw.write(prefix + "  id: " + sampleId + "\n");
            if (individual.getSex() != null) {
                pw.write(prefix + "  sex: " + individual.getSex().getSex().name() + "\n");
            }
            pw.write(prefix + "phenotypicFeatures:\n");
            for (String hpo : hpos) {
                pw.write(prefix + "  - type:\n");
                pw.write(prefix + "      id: " + hpo + "\n");
            }
            if (pedigree != null) {
                pw.write("pedigree:\n");
                pw.write("  persons:\n");

                // Proband
                pw.write("    - individualId: " + sampleId + "\n");
                if (pedigree.getProband().getFather() != null) {
                    pw.write("      paternalId: " + individual.getFather().getSamples().get(0).getId() + "\n");
                }
                if (pedigree.getProband().getMother() != null) {
                    pw.write("      maternalId: " + individual.getMother().getSamples().get(0).getId() + "\n");
                }
                if (pedigree.getProband().getSex() != null) {
                    pw.write("      sex: " + pedigree.getProband().getSex().getSex().name() + "\n");
                }
                pw.write("      affectedStatus: AFFECTED\n");

                // Father
                if (pedigree.getProband().getFather() != null) {
                    pw.write("    - individualId: " + individual.getFather().getSamples().get(0).getId() + "\n");
                    if (pedigree.getProband().getFather().getSex() != null) {
                        pw.write("      sex: " + pedigree.getProband().getFather().getSex().getSex().name() + "\n");
                    }
//                    pw.write("      - affectedStatus:" + AffectationStatus + "\n");
                }

                // Mother
                if (pedigree.getProband().getMother() != null) {
                    pw.write("    - individualId: " + individual.getMother().getSamples().get(0).getId() + "\n");
                    if (pedigree.getProband().getMother().getSex() != null) {
                        pw.write("      sex: " + pedigree.getProband().getMother().getSex().getSex().name() + "\n");
                    }
//                    pw.write("      - affectedStatus:" + AffectationStatus + "\n");
                }
            }

            // Close file
            pw.close();
        } catch (IOException e) {
            throw new ToolException("Error writing Exomiser sample file", e);
        }
        return  sampleFile;
    }

    private File createPedigreeFile(Family family, Pedigree pedigree) throws ToolException {
        Map<String, String> fromIndividualToSample = new HashMap<>();
        for (Individual member : family.getMembers()) {
            if (CollectionUtils.isNotEmpty(member.getSamples())) {
                fromIndividualToSample.put(member.getId(), member.getSamples().get(0).getId());
            }
        }

        File pedigreeFile = getOutDir().resolve(fromIndividualToSample.get(pedigree.getProband().getId()) + ".ped").toFile();
        try {
            PrintWriter pw = new PrintWriter(pedigreeFile);

            String probandId = fromIndividualToSample.get(pedigree.getProband().getId());

            String fatherId = "0";
            if (pedigree.getProband().getFather() != null) {
                fatherId = fromIndividualToSample.get(pedigree.getProband().getFather().getId());
            }

            String motherId = "0";
            if (pedigree.getProband().getMother() != null) {
                motherId = fromIndividualToSample.get(pedigree.getProband().getMother().getId());
            }

            // Proband
            pw.write(family.getId()
                    + "\t" + probandId
                    + "\t" + fatherId
                    + "\t" + motherId
                    + "\t" + getPedigreeSex(pedigree.getProband())
                    + "\t2\n");

            // Father
            if (!fatherId.equals("0")) {
                pw.write(family.getId() + "\t" + fatherId + "\t0\t0\t1\t0\n");
            }

            // Mother
            if (!motherId.equals("0")) {
                pw.write(family.getId() + "\t" + motherId + "\t0\t0\t2\t0\n");
            }

            // Close file
            pw.close();
        } catch (IOException e) {
            throw new ToolException("Error writing Exomiser pedigree file", e);
        }

        return pedigreeFile;
    }

    private String getPedigreePaternalId(Member member) {
        if (member.getFather() != null) {
            return member.getFather().getId();
        }
        return "0";
    }

    private String getPedigreeMaternalId(Member member) {
        if (member.getMother() != null) {
            return member.getMother().getId();
        }
        return "0";
    }

    private int getPedigreeSex(Member member) {
        if (member.getSex() != null) {
            if (member.getSex().getSex() == IndividualProperty.Sex.MALE) {
                return 1;
            } if (member.getSex().getSex() == IndividualProperty.Sex.FEMALE) {
                return 2;
            }
        }
        return 0;
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
        Path exomiserDataPath = openCgaHome.resolve("analysis/resources/" + ExomiserWrapperAnalysis.ID);
        if (!exomiserDataPath.toFile().exists()) {
            if (!exomiserDataPath.toFile().mkdirs()) {
                throw new ToolException("Error creating the Exomiser data directory");
            }
        }

        // Mutex management to avoid multiple downloadings at the same time
        // the first to come, download data, others have to wait for
        String resource = getToolResource(ExomiserWrapperAnalysis.ID, exomiserVersion, PHENOTYPE_RESOURCE_KEY);
        String resourceVersion = Paths.get(resource).getFileName().toString().split("[_]")[0];
        File readyFile = exomiserDataPath.resolve("READY-" + resourceVersion).toFile();
        File preparingFile = exomiserDataPath.resolve("PREPARING-" + resourceVersion).toFile();

        // If all is ready, then return
        if (readyFile.exists()) {
            logger.info("Exomiser {} data {} is already downloaded, so Exomiser analysis is ready to be executed.", exomiserVersion,
                    resourceVersion);
            return exomiserDataPath;
        }

        // If it is preparing, then wait for ready and then return
        if (preparingFile.exists()) {
            long startTime = System.currentTimeMillis();
            logger.info("Exomiser {} data {} is downloading, waiting for it...", exomiserVersion, resourceVersion);
            while (!readyFile.exists()) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    // Nothing to do here
                    preparingFile.delete();
                    throw new ToolException(e);
                }
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (elapsedTime > 18000000) {
                    throw new ToolException("Unable to run the Exomiser analysis because of Exomiser " + exomiserVersion + " data "
                            + resourceVersion + " is not ready yet: maximum waiting time exceeded");
                }
            }
            logger.info("Exomiser {} data is now downloaded: Exomiser analysis is ready to be executed", exomiserVersion);
            return exomiserDataPath;
        }

        // Mark as preparing
        try {
            preparingFile.createNewFile();
        } catch (IOException e) {
            preparingFile.delete();
            throw new ToolException("Error creating the Exomiser " + exomiserVersion + " data " + resourceVersion + " directory");
        }

        // Download resources and unzip files
        try {
            downloadAndUnzip(exomiserDataPath, HG38_RESOURCE_KEY);
            downloadAndUnzip(exomiserDataPath, PHENOTYPE_RESOURCE_KEY);
        } catch (ToolException e) {
            // If something wrong happened, the preparing file has to be deleted
            preparingFile.delete();
            throw new ToolException("Something wrong happened when preparing Exomiser " + exomiserVersion + " data " + resourceVersion, e);
        }

        // Mutex management, signal exomiser data is ready
        try {
            readyFile.createNewFile();
        } catch (IOException e) {
            throw new ToolException("Error preparing Exomiser " + exomiserVersion + " data " + resourceVersion, e);
        }
        preparingFile.delete();

        return exomiserDataPath;
    }

    private String getHg38DataVersion() throws ToolException {
        String resource = getToolResource(ExomiserWrapperAnalysis.ID, exomiserVersion, HG38_RESOURCE_KEY);
        return Paths.get(resource).getFileName().toString().split("_")[0];
    }

    private String getPhenotypeDataVersion() throws ToolException {
        String resource = getToolResource(ExomiserWrapperAnalysis.ID, exomiserVersion, PHENOTYPE_RESOURCE_KEY);
        return Paths.get(resource).getFileName().toString().split("_")[0];
    }

    @Override
    public String getDockerImageName() throws ToolException {
        return getDockerImageName(ExomiserWrapperAnalysis.ID, exomiserVersion);
    }

    @Override
    public String getDockerImageVersion() throws ToolException {
        return getDockerImageVersion(ExomiserWrapperAnalysis.ID, exomiserVersion);
    }

    public String getStudyId() {
        return studyId;
    }

    public ExomiserWrapperAnalysisExecutor setStudyId(String studyId) {
        this.studyId = studyId;
        return this;
    }

    private void downloadAndUnzip(Path exomiserDataPath, String resourceKey) throws ToolException {
        String filename;
        String resource = getToolResource(ExomiserWrapperAnalysis.ID, exomiserVersion, resourceKey);
        if (resource.startsWith("file://")) {
            // Copy resouce
            try {
                Path sourcePath = Paths.get(resource);
                filename = sourcePath.getFileName().toString();
                Files.copy(sourcePath, exomiserDataPath.resolve(filename));
            } catch (IOException e) {
                throw new ToolException("Error copying Exomiser data from " + resource, e);
            }
        } else {
            // Download resource
            String url;
            if (resource.startsWith("http://") || resource.startsWith("https://") || resource.startsWith("ftp://")) {
                url = resource;
            } else {
                url = getConfiguration().getAnalysis().getResourceUrl() + resource;
            }
            logger.info("Downloading Exomiser data: {} in {}", url, exomiserDataPath);
            try {
                ResourceUtils.downloadThirdParty(new URL(url), exomiserDataPath);
                filename = Paths.get(url).getFileName().toString();
            } catch (IOException e) {
                throw new ToolException("Error downloading Exomiser data from " + url, e);
            }
        }

        // Unzip
        try {
            logger.info("Decompressing Exomiser {} data: {}", exomiserDataPath, filename);
            new Command("unzip -o -d " + exomiserDataPath + " " + exomiserDataPath + "/" + filename)
                    .setOutputOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stdout_unzip_"
                            + filename + ".txt").toFile())))
                    .setErrorOutputStream(new DataOutputStream(new FileOutputStream(getOutDir().resolve("stderr_unzip_"
                            + filename + ".txt").toFile())))
                    .run();
        } catch (FileNotFoundException e) {
            throw new ToolException("Error unzipping Exomiser " + exomiserVersion + " data: " + filename, e);
        }

        // Free disk space
        logger.info("Deleting Exomiser data: {}", filename);
        exomiserDataPath.resolve(filename).toFile().delete();
    }

    public String getSampleId() {
        return sampleId;
    }

    public ExomiserWrapperAnalysisExecutor setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getExomiserVersion() {
        return exomiserVersion;
    }

    public ExomiserWrapperAnalysisExecutor setExomiserVersion(String exomiserVersion) {
        this.exomiserVersion = exomiserVersion;
        return this;
    }
}

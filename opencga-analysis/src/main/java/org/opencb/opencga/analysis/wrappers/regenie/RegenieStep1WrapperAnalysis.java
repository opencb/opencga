/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.wrappers.regenie;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.regenie.RegenieStep1WrapperParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.wrappers.executors.DockerWrapperAnalysisExecutor.*;
import static org.opencb.opencga.analysis.wrappers.regenie.RegenieUtils.*;
import static org.opencb.opencga.core.tools.ResourceManager.RESOURCES_DIRNAME;

@Tool(id = RegenieStep1WrapperAnalysis.ID, resource = Enums.Resource.VARIANT, description = RegenieStep1WrapperAnalysis.DESCRIPTION)
public class RegenieStep1WrapperAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "regenie-step1";
    public static final String DESCRIPTION = "Regenie is a program for whole genome regression modelling of large genome-wide association"
            + " studies. This performs the step1 of the regenie analysis.";


    private static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    private static final String CLEAN_RESOURCES_STEP = "clean-resources";
    private static final String PREPARE_WALKER_DOCKER_STEP = "build-and-push-walker-docker";

    private ObjectMap regenieOptions = new ObjectMap();

    private Path vcfFile;

    private String variantsSource;
    private String phenotypeSource;

    private List<String> caseSamples;
    private List<String> controlSamples;

    private RegenieStep1WrapperAnalysisExecutor executor;

    private String dockerName;
    private String dockerTag;
    private String dockerUsername;
    private String dockerPassword;

    private String dockerBasename;

    private Path resourcePath;

    @ToolParams
    protected final RegenieStep1WrapperParams regenieParams = new RegenieStep1WrapperParams();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);
        executor = getToolExecutor(RegenieStep1WrapperAnalysisExecutor.class);
        ObjectMap options = regenieParams.getRegenieParams();

        // Check input variants from BGEN, BED, PGEN, VCF or query
        checkVariants(options);

        // Check phenotype from phenotype file, case/control cohorts or phenotype ID
        checkPhenotype(options);

        // Check regenie options
        checkRegenieOptions(options);

        // Check doker parameters (name, username and password)
        checkDockerParameters();

        addAttribute("OPENCGA_STEP1_REGENIE_PARAMETERS", regenieParams);
    }

    @Override
    protected List<String> getSteps() {
        return Arrays.asList(PREPARE_RESOURCES_STEP, ID, PREPARE_WALKER_DOCKER_STEP, CLEAN_RESOURCES_STEP);
    }

    protected void run() throws ToolException, IOException {
        // Prepare regenie resource files in the job dir
        step(PREPARE_RESOURCES_STEP, this::prepareResources);

        // Run regenie
        step(ID, this::runRegenieStep1);

        // Do we have to clean the regenie resource folder
        step(PREPARE_WALKER_DOCKER_STEP, this::prepareWalkerDocker);

        // Do we have to clean the regenie resource folder
        step(CLEAN_RESOURCES_STEP, this::cleanResources);
    }

    private void prepareResources() throws ToolException {
        // Create folder where the regenie resources will be saved (within the job dir, aka outdir)
        try {
            resourcePath = Files.createDirectories(getOutDir().resolve(RESOURCES_DIRNAME));

            ObjectMap options = regenieParams.getRegenieParams();

            // Copy all files to the resources; and add other options
            for (String key : options.keySet()) {
                if (ALL_FILE_OPTIONS.contains(key)) {
                    Path path = checkRegenieInputFile(options.getString(key), false, "Option " + key + " file ", study, catalogManager,
                            token);
                    Path dest = resourcePath.resolve(path.getFileName());
                    FileUtils.copyFile(path, dest);
                    if (!Files.exists(dest)) {
                        throw new ToolException("File " + dest + " not found after copy (preparing resources)");
                    }
                    regenieOptions.put(key, dest.toAbsolutePath().toString());
                } else {
                    // Ohter options
                    regenieOptions.put(key, options.get(key));
                }
            }

            // Prepare variants
            prepareVariants();

            // Prepare phenotype
            prepareGenotype();
        } catch (ToolException | IOException | CatalogException | StorageEngineException e) {
            clean();
            throw new ToolException(e);
        }
    }

    private void runRegenieStep1() throws ToolException {
        try {
            // Set parameters and execute
            executor.setStudy(study)
                    .setOptions(regenieOptions)
                    .setOutputPath(getOutDir())
                    .execute();

            logger.info("Regenie step1 done!");
        } catch (ToolException e) {
            clean();
            throw e;
        }
    }

    private void cleanResources() throws IOException {
        FileUtils.deleteDirectory(resourcePath);
    }

    private void checkVariants(ObjectMap inputOptions) throws ToolException, CatalogException {
        variantsSource = "";
        if (MapUtils.isNotEmpty(inputOptions) && (inputOptions.containsKey(BGEN_OPTION) || inputOptions.containsKey(BED_OPTION)
                || inputOptions.containsKey(PGEN_OPTION))) {
            checkBgenBedPgenOptions(inputOptions);
        }
        if (StringUtils.isEmpty(variantsSource) && StringUtils.isNotEmpty(regenieParams.getVcfFile())) {
            vcfFile = checkRegenieInputFile(regenieParams.getVcfFile(), false, "VCF", study, catalogManager, token);
            logger.info("Variants from the BGEN file {}", vcfFile);
            variantsSource = "vcf";
        }
        if (StringUtils.isEmpty(variantsSource) && !MapUtils.isEmpty(regenieParams.getVariantExportQuery())) {
            if (logger.isInfoEnabled()) {
                logger.info("Variants from the query {}", regenieParams.getVariantExportQuery().toJson());
            }
            variantsSource = "query";
        }
        if (StringUtils.isEmpty(variantsSource)) {
            throw new ToolException("Missing variants data used in the regenie step1. Please, use the options " + BGEN_OPTION + ", "
                    + BED_OPTION + ", " + PGEN_OPTION + " or provide a VCF file or a query to export the variants");
        }
    }

    private void checkPhenotype(ObjectMap inputOptions) throws ToolException, CatalogException {
        if (MapUtils.isNotEmpty(inputOptions) && inputOptions.containsKey(PHENO_FILE_OPTION)) {
            phenotypeSource = PHENO_FILE_OPTION;
            checkRegenieInputFile(inputOptions.getString(PHENO_FILE_OPTION), true, "Phenotype", study, catalogManager, token);
        }
        if (StringUtils.isEmpty(phenotypeSource)) {
            if (StringUtils.isNotEmpty(regenieParams.getCaseCohort()) && StringUtils.isNotEmpty(regenieParams.getControlCohort())) {
                checkPhenotypeFromCohorts();
            } else if (StringUtils.isNotEmpty(regenieParams.getPhenotype())) {
                checkPhenotypeFromPhenotypeID();
            }
        }
        if (StringUtils.isEmpty(phenotypeSource)) {
            throw new ToolException("Missing phenotype data. Please, use the option " + PHENO_FILE_OPTION + " or provide the case and"
                    + " control cohort names, or a phenotype ID");
        }
    }

    private void checkRegenieOptions(ObjectMap inputOptions) throws ToolException {
        if (MapUtils.isNotEmpty(inputOptions)) {
            for (String key : inputOptions.keySet()) {
                if (ALL_FILE_OPTIONS.contains(key)) {
                    String value = inputOptions.getString(key);
                    if (StringUtils.isEmpty(value)) {
                        throw new ToolException(key + " is a file option, so its value must not be empty");
                    }
                    if (!value.startsWith(FILE_PREFIX)) {
                        throw new ToolException(key + " is a file option, so its value must start with " + FILE_PREFIX
                                + ". Current value: " + value);
                    }
                    // Check covar file without phenotype file
                    if (key.equals(COVAR_FILE_OPTION) && !inputOptions.containsKey(PHENO_FILE_OPTION))
                        throw new ToolException("Missing phenotype file. The covar file option " + COVAR_FILE_OPTION
                                + " requires the phenotype file option " + PHENO_FILE_OPTION);
                    value = value.substring(FILE_PREFIX.length());
                    checkRegenieInputFile(value, false, key, study, catalogManager, token);
                } else {
                    Object value = inputOptions.get(key);
                    if ((value instanceof String) && value.toString().startsWith(FILE_PREFIX)) {
                        throw new ToolException(key + " is not a file option, so its value must not start with " + FILE_PREFIX + ". Current"
                                + " value: " + value);
                    }
                }
            }
        }
    }

    private void checkDockerParameters() throws ToolException {
        if (regenieParams.getDocker() != null) {
            dockerName = checkRegenieInputParameter(regenieParams.getDocker().getName(), false, "Docker name");
            dockerTag = checkRegenieInputParameter(regenieParams.getDocker().getTag(), false, "Docker tag");
            dockerUsername = checkRegenieInputParameter(regenieParams.getDocker().getUsername(), false, "Docker Hub username");
            dockerPassword = checkRegenieInputParameter(regenieParams.getDocker().getPassword(), false, "Docker Hub password (or personal"
                    + " access token)");

            if (StringUtils.isNotEmpty(dockerName) && !dockerName.contains("/")) {
                throw new ToolException("Invalid docker name " + dockerName + ", please, provide: namespace/repository");
            }

            if (StringUtils.isNotEmpty(dockerName) && (StringUtils.isEmpty(dockerUsername) || StringUtils.isEmpty(dockerPassword))) {
                throw new ToolException("Docker username and password (or personal access token) are required if docker name is provided");
            }

            if (StringUtils.isNotEmpty(dockerUsername) && (StringUtils.isEmpty(dockerName) || StringUtils.isEmpty(dockerPassword))) {
                throw new ToolException("Docker name and password (or personal access token) are required if docker username is provided");
            }

            if (StringUtils.isNotEmpty(dockerPassword) && (StringUtils.isEmpty(dockerName) || StringUtils.isEmpty(dockerUsername))) {
                throw new ToolException("Docker name and username are required if docker password (or personal access token) is provided");
            }
        }

        // Get docker base name
        dockerBasename = configuration.getAnalysis().getOpencgaExtTools();
        if (StringUtils.isEmpty(dockerBasename)) {
            throw new ToolException("Docker base name is not set, please, check your configuration file");
        }
        if (!dockerBasename.contains(":")) {
            dockerBasename += ":" + GitRepositoryState.getInstance().getBuildVersion();
        }
    }

    private void checkBgenBedPgenOptions(ObjectMap inputOptions) throws ToolException, CatalogException {
        Path path = checkRegenieInputFile(inputOptions.getString(BGEN_OPTION), false, "BGEN", study, catalogManager, token);
        if (path != null) {
            logger.info("Variants from the BGEN file {}", path);
            regenieOptions.put(BGEN_OPTION, path.toAbsolutePath().toString());
            inputOptions.remove(BED_OPTION);
            inputOptions.remove(PGEN_OPTION);
            variantsSource = BGEN_OPTION;
        }

        // Check BED, BIM and FAM files
        path = checkInputFiles(inputOptions.getString(BED_OPTION), ".bed", Arrays.asList(".bim", ".fam"));
        if (path != null) {
            logger.info("Variants from the BED file {}", path);
            regenieOptions.put(BED_OPTION, path.toAbsolutePath().toString());
            inputOptions.remove(BGEN_OPTION);
            inputOptions.remove(PGEN_OPTION);
            variantsSource = BED_OPTION;
        }

        // Check PGEN, PVAR and PSAM files
        path = checkInputFiles(inputOptions.getString(PGEN_OPTION), ".pgen", Arrays.asList(".pvar", ".psam"));
        if (path != null) {
            logger.info("Variants from the PGEN file {}", path);
            regenieOptions.put(PGEN_OPTION, path.toAbsolutePath().toString());
            inputOptions.remove(BGEN_OPTION);
            inputOptions.remove(BED_OPTION);
            variantsSource = PGEN_OPTION;
        }
    }

    private void checkPhenotypeFromPhenotypeID() throws CatalogException, ToolException {
        // Get case samples from phenotype ID
        Query query = new Query(IndividualDBAdaptor.QueryParams.PHENOTYPES.key(),
                Collections.singletonList(regenieParams.getPhenotype()));
        OpenCGAResult<Individual> individuals = catalogManager.getIndividualManager().search(study, query, QueryOptions.empty(),
                token);
        if (individuals.getNumResults() == 0) {
            throw new ToolException("Phenotype " + regenieParams.getPhenotype() + " not found in the OpenCGA catalog individuals");
        }
        caseSamples = individuals.getResults().stream().filter(i -> CollectionUtils.isEmpty(i.getSamples()))
                .map(i -> i.getSamples().get(0).getId()).collect(Collectors.toList());
        if (caseSamples.size() < MINIMUN_NUM_SAMPLES) {
            throw new ToolException("Insufficient number of 'case' samples (" + caseSamples.size() + ") from individual phenotype "
                    + regenieParams.getPhenotype() + ". Minimum number of samples: " + MINIMUN_NUM_SAMPLES);
        }

        // Get control samples
        individuals = catalogManager.getIndividualManager().search(study, new Query(), QueryOptions.empty(), token);
        controlSamples = individuals.getResults().stream().filter(i -> CollectionUtils.isEmpty(i.getSamples()))
                .map(i -> i.getSamples().get(0).getId()).collect(Collectors.toList());
        controlSamples.removeAll(caseSamples);
        if (controlSamples.size() < MINIMUN_NUM_SAMPLES) {
            throw new ToolException("Insufficient number of 'control' samples (" + controlSamples.size() + ") from individual"
                    + " phenotype " + regenieParams.getPhenotype() + ". Minimum number of samples: " + MINIMUN_NUM_SAMPLES);
        }

        // Set phenotype source
        phenotypeSource = "phenotype";
    }

    private void checkPhenotypeFromCohorts() throws CatalogException, ToolException {
        // Get samples from case and control cohorts
        OpenCGAResult<Sample> samples = catalogManager.getCohortManager().getSamples(study, regenieParams.getCaseCohort(), token);
        caseSamples = samples.getResults().stream().map(Sample::getId).collect(Collectors.toList());
        samples = catalogManager.getCohortManager().getSamples(study, regenieParams.getControlCohort(), token);
        controlSamples = samples.getResults().stream().map(Sample::getId).collect(Collectors.toList());
        Collection<String> intersection = CollectionUtils.intersection(caseSamples, controlSamples);
        if (CollectionUtils.isNotEmpty(intersection)) {
            caseSamples = caseSamples.stream().filter(s -> !intersection.contains(s)).collect(Collectors.toList());
            controlSamples = controlSamples.stream().filter(s -> !intersection.contains(s)).collect(Collectors.toList());
        }

        if (caseSamples.size() < MINIMUN_NUM_SAMPLES) {
            throw new ToolException("Insufficient number of samples (" + caseSamples.size() + ") from case cohort "
                    + regenieParams.getCaseCohort() + " after removing duplicated samples in both cohorts. Minimum number of"
                    + " samples: " + MINIMUN_NUM_SAMPLES);
        }
        if (controlSamples.size() < MINIMUN_NUM_SAMPLES) {
            throw new ToolException("Insufficient number of samples (" + controlSamples.size() + ") from control cohort "
                    + regenieParams.getControlCohort() + " after removing duplicated samples in both cohorts. Minimum number of"
                    + " samples: " + MINIMUN_NUM_SAMPLES);
        }

        // Set phenotype source
        phenotypeSource = "cohorts";
    }

    private void prepareGenotype() throws IOException, ToolException {
        switch (phenotypeSource) {
            case PHENO_FILE_OPTION: {
                // Nothing to do: the file is already copied
                break;
            }

            case "cohorts":
            case "phenotype": {
                // Create phenotype file
                Path phenoFile = resourcePath.resolve(PHENO_FILENAME);
                try (BufferedWriter bw = FileUtils.newBufferedWriter(phenoFile)) {
                    for (String sample : caseSamples) {
                        bw.write(sample + "\t1\n");
                    }
                    for (String sample : controlSamples) {
                        bw.write(sample + "\t0\n");
                    }
                }
                break;
            }

            default: {
                throw new ToolException("Unknown phenotype source: " + phenotypeSource);
            }
        }
    }

    private void prepareVariants() throws IOException, ToolException, CatalogException, StorageEngineException {
        switch (variantsSource) {
            case BGEN_OPTION:
            case BED_OPTION:
            case PGEN_OPTION: {
                // Nothing to do: the files are already copied
                break;
            }

            case "vcf": {
                FileUtils.copyFile(vcfFile, resourcePath.resolve(vcfFile.getFileName()));
                if (!Files.exists(resourcePath.resolve(vcfFile.getFileName()))) {
                    throw new ToolException("VCF file not found in the resources folder after copy");
                }
                vcfFile = resourcePath.resolve(vcfFile.getFileName());
                break;
            }

            case "query": {
                // Export variants
                vcfFile = resourcePath.resolve(VCF_FILENAME);
                VariantQuery variantQuery = new VariantQuery()
                        .study(getStudy())
                        .includeSampleAll()
                        .includeSampleData("GT,FT")
                        .unknownGenotype("0/0");
                if (MapUtils.isNotEmpty(regenieParams.getVariantExportQuery())) {
                    variantQuery.putAll(regenieParams.getVariantExportQuery());
                }
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "id,studies.samples");
                logger.info("Variant export query: {}", MapUtils.isEmpty(variantQuery) ? null : variantQuery.toJson());
                getVariantStorageManager().exportData(vcfFile.toString(), VariantWriterFactory.VariantOutputFormat.VCF_GZ, null,
                        variantQuery, queryOptions, getToken());
                if (!Files.exists(vcfFile)) {
                    throw new ToolException("Exported VCF file not found");
                }
                break;
            }

            default: {
                throw new ToolException("Unknown phenotype source: " + phenotypeSource);
            }
        }
        if (vcfFile != null) {
            makeBedFile();
        }
    }

    private void prepareWalkerDocker() throws ToolException {
        Map<String, Object> params = new HashMap<>(regenieParams.toParams());
        logger.info("Building and pushing regenie-walker docker with parameters {} ...", params);

        try {
            // Prepare regenie-step1 results to be included in the docker image
            Path dataDir = getOutDir().resolve(ID);
            if (!Files.exists(Files.createDirectories(dataDir))) {
                throw new ToolException("Could not create directory " + dataDir);
            }

            // Copy pheno and covar files
            FileUtils.copyDirectory(resourcePath, dataDir);

            // Copy Python scripts and files
            Path pythonDir = dataDir.resolve("python");
            List<String> filenames = Arrays.asList("requirements.txt", "variant_walker.py", "regenie_walker.py");
            for (String filename : filenames) {
                FileUtils.copyFile(getOpencgaHome().resolve("analysis/regenie/" + filename).toAbsolutePath(),
                        pythonDir.resolve(filename));
            }
            // Copy step1 results (i.e., prediction files) and update the paths within the file step1_pred.list
            Path predDir = dataDir.resolve("pred");
            if (!Files.exists(Files.createDirectories(predDir))) {
                throw new ToolException("Could not create directory " + predDir);
            }
            Path step1PredPath = getOutDir().resolve(STEP1_PRED_LIST_FILNEMANE);
            if (!Files.exists(step1PredPath)) {
                throw new ToolException("Could not find the regenie-step1 predictions file: " + STEP1_PRED_LIST_FILNEMANE);
            }
            FileUtils.copyFile(getOutDir().resolve(STEP1_PRED_LIST_FILNEMANE), predDir.resolve(STEP1_PRED_LIST_FILNEMANE));
            List<String> lines = Files.readAllLines(step1PredPath);
            try (BufferedWriter bw = FileUtils.newBufferedWriter(predDir.resolve(STEP1_PRED_LIST_FILNEMANE))) {
                for (String line : lines) {
                    String[] split = line.split(" ");
                    String locoFilename = Paths.get(split[1]).getFileName().toString();
                    Path locoPath = getOutDir().resolve(locoFilename);
                    if (!Files.exists(locoPath)) {
                        throw new ToolExecutorException("Could not find the regenie-step1 loco file: " + locoPath.getFileName());
                    }
                    FileUtils.copyFile(locoPath, predDir.resolve(locoFilename));
                    bw.write(split[0] + " " + OPT_APP_PRED_VIRTUAL_DIR + "/" + locoFilename + "\n");
                }
            }

            if (StringUtils.isEmpty(dockerName) && StringUtils.isEmpty(dockerUsername) && StringUtils.isEmpty(dockerPassword)) {
                logger.info("Creating regenie-walker Dockerfile ...");
                Path dockerfile = createDockerfile(dataDir, dockerBasename, getOpencgaHome());
                logger.info("Dockerfile for regenie-walker: {}", dockerfile);
            } else {
                logger.info("Building and pushing regenie-walker ...");
                String walkerDocker = buildAndPushDocker(dataDir, dockerBasename, dockerName, dockerTag, dockerUsername, dockerPassword,
                        getOpencgaHome());

                logger.info("Regenie-walker docker image: {}", walkerDocker);
                addAttribute(OPENCGA_REGENIE_WALKER_DOCKER_IMAGE_KEY, walkerDocker);
                logger.info("Building and pushing regenie-walker docker done!");

                // Clean up
                FileUtils.deleteDirectory(dataDir);
            }
        } catch (ToolException | IOException e) {
            clean();
            throw new ToolException(e);
        }
    }

    private void clean() {
        List<Path> paths = Arrays.asList(getOutDir().resolve(ID), getOutDir().resolve(RESOURCES_DIRNAME));
        for (Path path : paths) {
            if (Files.exists(path)) {
                try {
                    logger.info("Cleaning after error: deleting directory {}", path);
                    FileUtils.deleteDirectory(path);
                } catch (IOException e) {
                    logger.error("Error deleting directory {}: {}", path, e.getMessage(), e);
                }
            }
        }
    }

    private Path checkInputFiles(String inputFilename, String mainExtension, List<String> extensions) throws ToolException, CatalogException {
        Path inputPath = checkRegenieInputFile(inputFilename, false, mainExtension, getStudy(), getCatalogManager(), getToken());
        if (inputPath != null) {
            String filename = inputPath.toFile().getName();
            if (!filename.endsWith(mainExtension)) {
                throw new ToolException("Invalid file name: " + filename + ", it must end with " + mainExtension);
            }
            String basename = filename.substring(0, filename.length() - mainExtension.length());
            for (String extension : extensions) {
                // Check if the file with the same basename and extension exists
                String name = basename + extension;
                Query query = new Query("name", name);
                OpenCGAResult<File> results = catalogManager.getFileManager().search(study, query, QueryOptions.empty(), token);
                if (results.getNumResults() == 0) {
                    throw new ToolException("File " + name + " not found in OpenCGA catalog");
                }
                boolean found = false;
                for (File file: results.getResults()) {
                    Path path = Paths.get(file.getUri().getPath()).toAbsolutePath();
                    if (path.getParent() == inputPath.getParent()) {
                        if (Files.exists(path)) {
                            found = true;
                            break;
                        }
                        throw new ToolException("File " + name + " found in the OpenCGA catalog but does not exist physically at "
                                + path.getParent());
                    }
                }
                if (!found) {
                    throw new ToolException("Any file " + name + " found is located next to the " + mainExtension + " file ("
                            + filename + ")");
                }
            }
        }
        return inputPath;
    }

    private void makeBedFile() throws ToolException, IOException {
        Path step1ScriptPath = getOpencgaHome().resolve("analysis/regenie/make_bed.sh");

        String basename;
        String vcfFilename = vcfFile.getFileName().toString();
        if (vcfFilename.endsWith(".vcf")) {
            basename = vcfFilename.substring(0, vcfFilename.length() - ".vcf".length());
        } else if (vcfFilename.endsWith(".vcf.gz")) {
            basename = vcfFilename.substring(0, vcfFilename.length() - ".vcf.gz".length());
        } else {
            throw new ToolException("Invalid VCF file name: " + vcfFilename + ". It must end with .vcf or .vcf.gz");
        }

        // Input bindings
        List<AbstractMap.SimpleEntry<String, String>> inputBindings = new ArrayList<>();
        Path virtualScriptPath = Paths.get(SCRIPT_VIRTUAL_PATH).resolve(step1ScriptPath.getFileName());
        inputBindings.add(new AbstractMap.SimpleEntry<>(step1ScriptPath.toAbsolutePath().toString(), virtualScriptPath.toString()));
        inputBindings.add(new AbstractMap.SimpleEntry<>(resourcePath.toAbsolutePath().toString(), INPUT_VIRTUAL_PATH));

        // Read only input bindings
        Set<String> readOnlyInputBindings = new HashSet<>();
        readOnlyInputBindings.add(virtualScriptPath.toString());

        // Output binding
        AbstractMap.SimpleEntry<String, String> outputBinding = new AbstractMap.SimpleEntry<>(resourcePath.toAbsolutePath().toString(),
                OUTPUT_VIRTUAL_PATH);

        // Main command line and params
        String params = "bash " + virtualScriptPath
                + " " + INPUT_VIRTUAL_PATH
                + " " + vcfFilename
                + " " + OUTPUT_VIRTUAL_PATH;

        // Execute Pythong script in docker
        String dockerImage = executor.getDockerImageName() + ":" + executor.getDockerImageVersion();

        String dockerCli = executor.buildCommandLine(dockerImage, inputBindings, readOnlyInputBindings, outputBinding, params, null);
        logger.info("Docker command line: {}", dockerCli);
        executor.runCommandLine(dockerCli);

        // Check result files
        String suffix = ".annotated.pruned.in";
        for (String extension: Arrays.asList(".bed", ".bim", ".fam")) {
            Path file = Paths.get(resourcePath.toString(), basename + suffix + extension);
            if (!Files.exists(file)) {
                throw new ToolException("Expected file " + file + " not found.");
            }
        }
        regenieOptions.put(BED_OPTION, Paths.get(resourcePath.toString(), basename + suffix + ".bed").toAbsolutePath().toString());
    }
}

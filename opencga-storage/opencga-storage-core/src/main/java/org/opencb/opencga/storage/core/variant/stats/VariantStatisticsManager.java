package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantDBReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 12/02/15.
 */
public class VariantStatisticsManager {

    public static final String BATCH_SIZE = "batchSize";
    private String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";
    private String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";
    private final JsonFactory jsonFactory;
    private ObjectMapper jsonObjectMapper;
    protected static Logger logger = LoggerFactory.getLogger(VariantStatisticsManager.class);

    public VariantStatisticsManager() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
    }

    /**
     * retrieves batches of Variants, delegates to obtain VariantStatsWrappers from those Variants, and writes them to the output URI.
     *
     * @param variantDBAdaptor to obtain the Variants
     * @param output where to write the VariantStats
     * @param cohorts cohorts (subsets) of the samples. key: cohort name, value: list of sample names.
     * @param options filters to the query, batch size, number of threads to use...
     *
     * @return outputUri prefix for the file names (without the "._type_.stats.json.gz")
     * @throws IOException
     */
    public URI legacyCreateStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts, StudyConfiguration studyConfiguration, QueryOptions options) throws IOException {

        /** Open output streams **/
        Path fileVariantsPath = Paths.get(output.getPath() + VARIANT_STATS_SUFFIX);
        OutputStream outputVariantsStream = getOutputStream(fileVariantsPath, options);
//        PrintWriter printWriter = new PrintWriter(getOutputStream(fileVariantsPath, options));

        Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
        OutputStream outputSourceStream = getOutputStream(fileSourcePath, options);

        /** Initialize Json serializer **/
        ObjectWriter variantsWriter = jsonObjectMapper.writerFor(VariantStatsWrapper.class);
        ObjectWriter sourceWriter = jsonObjectMapper.writerFor(VariantSourceStats.class);

        /** Variables for statistics **/
        int batchSize = options.getInt(BATCH_SIZE, 1000); // future optimization, threads, etc
        boolean overwrite = options.getBoolean(VariantStorageManager.OVERWRITE_STATS, false);
        List<Variant> variantBatch = new ArrayList<>(batchSize);
        int retrievedVariants = 0;
        String fileId = options.getString(VariantStorageManager.FILE_ID);   //TODO: Change to int value
        String studyId = studyConfiguration.getStudyId() + "";                //TODO: Change to int value
//        VariantSource variantSource = options.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);   // TODO Is this retrievable from the adaptor?
        VariantSourceStats variantSourceStats = new VariantSourceStats(fileId, studyId);
        VariantStatisticsCalculator variantStatisticsCalculator = new VariantStatisticsCalculator(overwrite);
        boolean defaultCohortAbsent = false;

        logger.info("starting stats calculation");
        long start = System.currentTimeMillis();

        Iterator<Variant> iterator = obtainIterator(variantDBAdaptor, options);
        while (iterator.hasNext()) {
            Variant variant = iterator.next();
            retrievedVariants++;
            variantBatch.add(variant);
//            variantBatch.add(filterSample(variant, samples));

            if (variantBatch.size() == batchSize) {
                List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variantBatch, studyId, fileId, cohorts);

                for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                    outputVariantsStream.write(variantsWriter.writeValueAsBytes(variantStatsWrapper));
                    if (variantStatsWrapper.getCohortStats().get(VariantSourceEntry.DEFAULT_COHORT) == null) {
                        defaultCohortAbsent = true;
                    }
                }

                // we don't want to overwrite file stats regarding all samples with stats about a subset of samples. Maybe if we change VariantSource.stats to a map with every subset...
                if (!defaultCohortAbsent) {
                    variantSourceStats.updateFileStats(variantBatch);
                    variantSourceStats.updateSampleStats(variantBatch, null);  // TODO test
                }
                logger.info("stats created up to position {}:{}", variantBatch.get(variantBatch.size()-1).getChromosome(), variantBatch.get(variantBatch.size()-1).getStart());
                variantBatch.clear();
            }
        }

        if (variantBatch.size() != 0) {
            List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variantBatch, studyId, fileId, cohorts);
            for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                outputVariantsStream.write(variantsWriter.writeValueAsBytes(variantStatsWrapper));
                    if (variantStatsWrapper.getCohortStats().get(VariantSourceEntry.DEFAULT_COHORT) == null) {
                        defaultCohortAbsent = true;
                    }
            }

            if (!defaultCohortAbsent) {
                variantSourceStats.updateFileStats(variantBatch);
                variantSourceStats.updateSampleStats(variantBatch, null);  // TODO test
            }
            logger.info("stats created up to position {}:{}", variantBatch.get(variantBatch.size()-1).getChromosome(), variantBatch.get(variantBatch.size()-1).getStart());
            variantBatch.clear();
        }
        logger.info("finishing stats calculation, time: {}ms", System.currentTimeMillis() - start);
        if (variantStatisticsCalculator.getSkippedFiles() != 0) {
            logger.warn("the sources in {} (of {}) variants were not found, and therefore couldn't run its stats", variantStatisticsCalculator.getSkippedFiles(), retrievedVariants);
            logger.info("note: maybe the file-id and study-id were not correct?");
        }
        if (variantStatisticsCalculator.getSkippedFiles() == retrievedVariants) {
            throw new IllegalArgumentException("given fileId and studyId were not found in any variant. Nothing to write.");
        }
        outputSourceStream.write(sourceWriter.writeValueAsBytes(variantSourceStats));
        outputVariantsStream.close();
        outputSourceStream.close();
        return output;
    }
    private OutputStream getOutputStream(Path filePath, QueryOptions options) throws IOException {
        OutputStream outputStream = new FileOutputStream(filePath.toFile());
        logger.info("will write stats to {}", filePath);
        if(filePath.toString().endsWith(".gz")) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }

    /** Gets iterator from OpenCGA Variant database. **/
    private Iterator<Variant> obtainIterator(VariantDBAdaptor variantDBAdaptor, QueryOptions options) {

        QueryOptions iteratorQueryOptions = new QueryOptions();
        if(options != null) { //Parse query options
            iteratorQueryOptions = options;
        }

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "sourceEntries");
        iteratorQueryOptions.add("include", include);

        return variantDBAdaptor.iterator(iteratorQueryOptions);
    }

    /**
     * retrieves batches of Variants, delegates to obtain VariantStatsWrappers from those Variants, and writes them to the output URI.
     *
     * @param variantDBAdaptor to obtain the Variants
     * @param output where to write the VariantStats
     * @param samples cohorts (subsets) of the samples. key: cohort name, value: list of sample names.
     * @param options (mandatory) VariantSource, (optional) filters to the query, batch size, number of threads to use...
     *
     * @return outputUri prefix for the file names (without the "._type_.stats.json.gz")
     * @throws IOException
     */
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> samples, StudyConfiguration studyConfiguration, QueryOptions options) throws Exception {
        int numTasks = 6;
        int batchSize = 100;  // future optimization, threads, etc
        boolean overwrite = false;
        if(options != null) { //Parse query options
            batchSize = options.getInt(BATCH_SIZE, batchSize);
            overwrite = options.getBoolean(VariantStorageManager.OVERWRITE_STATS, overwrite);
        }

//        reader, tasks and writer
        VariantDBReader reader = new VariantDBReader(studyConfiguration, variantDBAdaptor, options);
        List<ParallelTaskRunner.Task<Variant, String>> tasks = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            tasks.add(new VariantStatsWrapperTask(overwrite, samples, studyConfiguration, options.getString(VariantStorageManager.FILE_ID)));
        }
        StringDataWriter writer = new StringDataWriter(Paths.get(output.getPath() + VARIANT_STATS_SUFFIX));
        
        // runner 
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, numTasks*2, false);
        ParallelTaskRunner runner = new ParallelTaskRunner<>(reader, tasks, writer, config);

        logger.info("starting stats creation");
        long start = System.currentTimeMillis();
        runner.run();
        logger.info("finishing stats creation, time: {}ms", System.currentTimeMillis() - start);
        
        return output;
    }

    class VariantStatsWrapperTask implements ParallelTaskRunner.Task<Variant, String> {

        private boolean overwrite;
        private Map<String, Set<String>> samples;
        private StudyConfiguration studyConfiguration;
        private String fileId;
        private ObjectMapper jsonObjectMapper;
        private ObjectWriter variantsWriter;

        public VariantStatsWrapperTask(boolean overwrite, Map<String, Set<String>> samples, StudyConfiguration studyConfiguration, String fileId) {
            this.overwrite = overwrite;
            this.samples = samples;
            this.studyConfiguration = studyConfiguration;
            this.fileId = fileId;
            jsonObjectMapper = new ObjectMapper(new JsonFactory());
            variantsWriter = jsonObjectMapper.writerFor(VariantStatsWrapper.class);
        }

        @Override
        public List<String> apply(List<Variant> variants) {

            List<String> strings = new ArrayList<>(variants.size());
            
            VariantStatisticsCalculator variantStatisticsCalculator = new VariantStatisticsCalculator(overwrite);
            List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variants, studyConfiguration.getStudyId()+"", fileId, samples);
            
            long start = System.currentTimeMillis();
            for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                try {
                    strings.add(variantsWriter.writeValueAsString(variantStatsWrapper));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            logger.debug("another batch  of {} elements calculated. time: {}ms", strings.size(), System.currentTimeMillis() - start);
            if (variants.size() != 0) {
                logger.info("stats created up to position {}:{}", variants.get(variants.size()-1).getChromosome(), variants.get(variants.size()-1).getStart());
            } else {
                logger.info("task with empty batch");
            }
            return strings;
        }
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options) throws IOException {

        URI variantStatsUri = Paths.get(uri.getPath() + VARIANT_STATS_SUFFIX).toUri();
        URI sourceStatsUri = Paths.get(uri.getPath() + SOURCE_STATS_SUFFIX).toUri();

        logger.info("starting stats loading from {} and {}", variantStatsUri, sourceStatsUri);
        long start = System.currentTimeMillis();

        loadVariantStats(variantDBAdaptor, variantStatsUri, studyConfiguration, options);
        loadSourceStats(variantDBAdaptor, sourceStatsUri, studyConfiguration, options);

        logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);
    }
    public void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        /** Initialize Json parse **/
        JsonParser parser = jsonFactory.createParser(variantInputStream);

        int batchSize = options.getInt(BATCH_SIZE, 1000);
        ArrayList<VariantStatsWrapper> statsBatch = new ArrayList<>(batchSize);
        int writes = 0;
        int variantsNumber = 0;

        while (parser.nextToken() != null) {
            variantsNumber++;
            statsBatch.add(parser.readValueAs(VariantStatsWrapper.class));

            if (statsBatch.size() == batchSize) {
                QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, studyConfiguration, options);
                writes += writeResult.getNumResults();
                logger.info("stats loaded up to position {}:{}", statsBatch.get(statsBatch.size()-1).getChromosome(), statsBatch.get(statsBatch.size()-1).getPosition());
                statsBatch.clear();
            }
        }

        if (!statsBatch.isEmpty()) {
            QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, studyConfiguration, options);
            writes += writeResult.getNumResults();
            logger.info("stats loaded up to position {}:{}", statsBatch.get(statsBatch.size()-1).getChromosome(), statsBatch.get(statsBatch.size()-1).getPosition());
            statsBatch.clear();
        }

        if (writes < variantsNumber) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantsNumber, writes);
            logger.info("note: maybe those variants didn't had the proper study? maybe the new and the old stats were the same?");
        }

    }
    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path sourceInput = Paths.get(uri.getPath());
        InputStream sourceInputStream;
        sourceInputStream = new FileInputStream(sourceInput.toFile());
        sourceInputStream = new GZIPInputStream(sourceInputStream);

        /** Initialize Json parse **/
        JsonParser sourceParser = jsonFactory.createParser(sourceInputStream);

        VariantSourceStats variantSourceStats;
//        if (sourceParser.nextToken() != null) {
        variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);
//        }

        // TODO if variantSourceStats doesn't have studyId and fileId, create another with variantSource.getStudyId() and variantSource.getFileId()
        variantDBAdaptor.getVariantSourceDBAdaptor().updateSourceStats(variantSourceStats, studyConfiguration, options);

    }

    /**
     * check that all SampleIds are in the StudyConfiguration
     *
     */
    public List<Integer> checkAndUpdateStudyConfigurationCohorts(StudyConfiguration studyConfiguration,
                                                        Map<String, Set<String>> cohorts, Map<String, Integer> cohortIds)
            throws IOException {
        List<Integer> cohortIdList = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Set<String> samples = entry.getValue();
            final int cohortId;

            if (cohortIds == null || cohortIds.isEmpty()) {
                //Auto-generate cohortId. Max CohortId + 1
                int maxCohortId = 0;
                for (int cid : studyConfiguration.getCohortIds().values()) {
                    if (cid > maxCohortId) {
                        maxCohortId = cid;
                    }
                }
                cohortId = maxCohortId + 1;
            } else {
                if (!cohortIds.containsKey(cohortName)) {
                    //ERROR Missing cohortId
                    throw new IOException("Missing cohortId for the cohort : " + cohortName);
                }
                cohortId = cohortIds.get(entry.getKey());
            }
            if (studyConfiguration.getCohortIds().containsKey(cohortName)) {
                //ERROR Duplicated cohortName
                throw new IOException("Duplicated cohortName " + cohortName + ":" + cohortId + ". Appears in the StudyConfiguration as " +
                        cohortName + ":" + studyConfiguration.getCohortIds().get(cohortName));
            }
            if (studyConfiguration.getCohortIds().containsValue(cohortId)) {
                //ERROR Duplicated cohortId
                throw new IOException("Duplicated cohortId " + cohortName + ":" + cohortId + ". Appears in the StudyConfiguration as " +
                        StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).get(cohortId) + ":" + cohortId);
            }

            Set<Integer> sampleIds = new HashSet<>();
            for (String sample : samples) {
                if (!studyConfiguration.getSampleIds().containsKey(sample)) {
                    //ERROR Sample not found
                    throw new IOException("Sample " + sample + " not found in the StudyConfiguration");
                } else {
                    sampleIds.add(studyConfiguration.getSampleIds().get(sample));
                }
            }
            if (sampleIds.size() != samples.size()) {
                throw new IOException("Duplicated samples in cohort " + cohortName + ":" + cohortId);
            }

            cohortIdList.add(cohortId);
            studyConfiguration.getCohortIds().put(cohortName, cohortId);
            studyConfiguration.getCohorts().put(cohortId, sampleIds);
        }
        return cohortIdList;
    }

}

/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.stats.VariantAggregatedStatsCalculator;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
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

import static org.opencb.biodata.models.variant.VariantSource.Aggregation.*;

/**
 * Created by jmmut on 12/02/15.
 */
public class VariantStatisticsManager {

    private String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";
    private String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";
    private final JsonFactory jsonFactory;
    private ObjectMapper jsonObjectMapper;
    protected static Logger logger = LoggerFactory.getLogger(VariantStatisticsManager.class);

    public VariantStatisticsManager() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixIn(VariantStats.class, VariantStatsJsonMixin.class);
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
    private Iterator<Variant> obtainIterator(VariantDBAdaptor variantDBAdaptor, Query query, QueryOptions options) {

        Query iteratorQuery = query != null ? query : new Query();
        //Parse query options
        QueryOptions iteratorQueryOptions = options != null ? options : new QueryOptions();

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternate", "reference", "sourceEntries");
        iteratorQueryOptions.add("include", include);

        return variantDBAdaptor.iterator(iteratorQuery, iteratorQueryOptions);
    }

    /**
     * retrieves batches of Variants, delegates to obtain VariantStatsWrappers from those Variants, and writes them to the output URI.
     *
     * steps:
     * 
     * * gets options like batchsize, overwrite, tagMap...  if(options == null) throws
     * * if no cohorts provided and the study is aggregated, uses the cohorts in the tagMap if any
     * * add the cohorts to the studyConfiuration
     * * checks invalidated stats, and set overwrite=true if needed
     * * sets up a ParallelTaskRunner: a reader, a writer and tasks
     * * writes the source stats
     * 
     * @param variantDBAdaptor to obtain the Variants
     * @param output where to write the VariantStats
     * @param cohorts cohorts (subsets) of the samples. key: cohort name, defaultValue: list of sample names.
     * @param cohortIds
     * @param options (mandatory) fileId, (optional) filters to the query, batch size, number of threads to use...
     *
     * @return outputUri prefix for the file names (without the "._type_.stats.json.gz")
     * @throws IOException
     */
    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, Map<String, Set<String>> cohorts,
                           Map<String, Integer> cohortIds, StudyConfiguration studyConfiguration, QueryOptions options)
            throws Exception {
        int numTasks = 6;
        int batchSize = 100;  // future optimization, threads, etc
        boolean overwrite = false;
        Properties tagmap = null;
//        String fileId;
        if(options != null) { //Parse query options
            batchSize = options.getInt(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), batchSize);
            numTasks = options.getInt(VariantStorageManager.Options.LOAD_THREADS.key(), numTasks);
            overwrite = options.getBoolean(VariantStorageManager.Options.OVERWRITE_STATS.key(), overwrite);
            tagmap = options.get(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), Properties.class);
//            fileId = options.getString(VariantStorageManager.Options.FILE_ID.key());
        } else {
            logger.error("missing required fileId in QueryOptions");
            throw new Exception("createStats: need a fileId to calculate stats from.");
        }

        // if no cohorts provided and the study is aggregated: try to get the cohorts from the tagMap
        if (cohorts == null) {
            if (isAggregated(studyConfiguration.getAggregation())) {
                cohorts = new LinkedHashMap<>();
                for (String c : VariantAggregatedStatsCalculator.getCohorts(tagmap)) {
                    cohorts.put(c, Collections.emptySet());
                }
            } else {
                cohorts = new LinkedHashMap<>();
            }
        }

        checkAndUpdateStudyConfigurationCohorts(studyConfiguration, cohorts, cohortIds);
        if (!overwrite) {
            for (String cohortName : cohorts.keySet()) {
                Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
                if (studyConfiguration.getInvalidStats().contains(cohortId)) {
                    logger.debug("Cohort \"{}\":{} is invalid. Need to overwrite stats. Using overwrite = true", cohortName, cohortId);
                    overwrite = true;
                }
            }
        }
        VariantStorageManager.checkStudyConfiguration(studyConfiguration);


        VariantSourceStats variantSourceStats = new VariantSourceStats(null/*FILE_ID*/, Integer.toString(studyConfiguration.getStudyId()));


       // reader, tasks and writer
        Query readerQuery = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyConfiguration.getStudyId());
        VariantDBReader reader = new VariantDBReader(studyConfiguration, variantDBAdaptor, readerQuery, null);
        List<ParallelTaskRunner.Task<Variant, String>> tasks = new ArrayList<>(numTasks);
        for (int i = 0; i < numTasks; i++) {
            tasks.add(new VariantStatsWrapperTask(overwrite, cohorts, studyConfiguration, null/*FILE_ID*/,
                    variantSourceStats, tagmap));
        }
        Path variantStatsPath = Paths.get(output.getPath() + VARIANT_STATS_SUFFIX);
        logger.info("will write stats to {}", variantStatsPath);
        StringDataWriter writer = new StringDataWriter(variantStatsPath);
        
        // runner 
        ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numTasks, batchSize, numTasks*2, false);
        ParallelTaskRunner runner = new ParallelTaskRunner<>(reader, tasks, writer, config);

        logger.info("starting stats creation");
        long start = System.currentTimeMillis();
        runner.run();
        logger.info("finishing stats creation, time: {}ms", System.currentTimeMillis() - start);

        // source stats
        Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
        OutputStream outputSourceStream = getOutputStream(fileSourcePath, options);
        ObjectWriter sourceWriter = jsonObjectMapper.writerFor(VariantSourceStats.class);
        outputSourceStream.write(sourceWriter.writeValueAsBytes(variantSourceStats));
        outputSourceStream.close();

        variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

        return output;
    }

    class VariantStatsWrapperTask implements ParallelTaskRunner.Task<Variant, String> {

        private boolean overwrite;
        private Map<String, Set<String>> samples;
        private StudyConfiguration studyConfiguration;
//        private String fileId;
        private ObjectMapper jsonObjectMapper;
        private ObjectWriter variantsWriter;
        private VariantSourceStats variantSourceStats;
        private Properties tagmap;
        private VariantStatisticsCalculator variantStatisticsCalculator;

        public VariantStatsWrapperTask(boolean overwrite, Map<String, Set<String>> samples,
                                       StudyConfiguration studyConfiguration, String fileId,
                                       VariantSourceStats variantSourceStats, Properties tagmap) {
            this.overwrite = overwrite;
            this.samples = samples;
            this.studyConfiguration = studyConfiguration;
//            this.fileId = fileId;
            jsonObjectMapper = new ObjectMapper(new JsonFactory());
            variantsWriter = jsonObjectMapper.writerFor(VariantStatsWrapper.class);
            this.variantSourceStats = variantSourceStats;
            this.tagmap = tagmap;
            variantStatisticsCalculator = new VariantStatisticsCalculator(overwrite);
            variantStatisticsCalculator.setAggregationType(studyConfiguration.getAggregation(), tagmap);
        }

        @Override
        public List<String> apply(List<Variant> variants) {

            List<String> strings = new ArrayList<>(variants.size());
            boolean defaultCohortAbsent = false;

            List<VariantStatsWrapper> variantStatsWrappers = variantStatisticsCalculator.calculateBatch(variants,
                    studyConfiguration.getStudyName(), null/*fileId*/, samples);

            long start = System.currentTimeMillis();
            for (VariantStatsWrapper variantStatsWrapper : variantStatsWrappers) {
                try {
                    strings.add(variantsWriter.writeValueAsString(variantStatsWrapper));
                    if (variantStatsWrapper.getCohortStats().get(VariantSourceEntry.DEFAULT_COHORT) == null) {
                        defaultCohortAbsent = true;
                    }
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
            // we don't want to overwrite file stats regarding all samples with stats about a subset of samples. Maybe if we change VariantSource.stats to a map with every subset...
            if (!defaultCohortAbsent) {
                synchronized (variantSourceStats) {
                    variantSourceStats.updateFileStats(variants);
                    variantSourceStats.updateSampleStats(variants, null);  // TODO test
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

        checkAndUpdateCalculatedCohorts(studyConfiguration, variantStatsUri);

        logger.info("starting stats loading from {} and {}", variantStatsUri, sourceStatsUri);
        long start = System.currentTimeMillis();

        loadVariantStats(variantDBAdaptor, variantStatsUri, studyConfiguration, options);
        loadSourceStats(variantDBAdaptor, sourceStatsUri, studyConfiguration, options);

        logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);

        variantDBAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, options);

    }

    public void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI uri, StudyConfiguration studyConfiguration, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        /** Initialize Json parse **/


        int batchSize = options.getInt(VariantStorageManager.Options.LOAD_BATCH_SIZE.key(), 1000);
        ArrayList<VariantStatsWrapper> statsBatch = new ArrayList<>(batchSize);
        int writes = 0;
        int variantsNumber = 0;

        try (JsonParser parser = jsonFactory.createParser(variantInputStream)) {
            while (parser.nextToken() != null) {
                variantsNumber++;
                statsBatch.add(parser.readValueAs(VariantStatsWrapper.class));

                if (statsBatch.size() == batchSize) {
                    QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, studyConfiguration, options);
                    writes += writeResult.getNumResults();
                    logger.info("stats loaded up to position {}:{}", statsBatch.get(statsBatch.size() - 1).getChromosome(), statsBatch.get(statsBatch.size() - 1).getPosition());
                    statsBatch.clear();
                }
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
     * If some cohort does not have samples, reads the content from StudyConfiguration.
     * If there is no cohortId for come cohort, reads the content from StudyConfiguration or auto-generate a cohortId
     * If some cohort has a different number of samples, check if this cohort is invalid.
     *
     * Do not update the "calculatedStats" array. Just check that the provided cohorts are not calculated or invalid.
     *
     * new requirements:
     * * an empty cohort is not an error if the study is aggregated
     * * there may be several empty cohorts, not just the ALL, because there may be several aggregated files with different sets of hidden samples.
     *
     * @return CohortIdList
     */
    List<Integer> checkAndUpdateStudyConfigurationCohorts(StudyConfiguration studyConfiguration,
                                                        Map<String, Set<String>> cohorts, Map<String, Integer> cohortIds)
            throws IOException {
        List<Integer> cohortIdList = new ArrayList<>();

        for (Map.Entry<String, Set<String>> entry : cohorts.entrySet()) {
            String cohortName = entry.getKey();
            Set<String> samples = entry.getValue();
            final int cohortId;


            // get a valid cohortId
            if (cohortIds == null || cohortIds.isEmpty()) {
                if (studyConfiguration.getCohortIds().containsKey(cohortName)) {
                    cohortId = studyConfiguration.getCohortIds().get(cohortName);
                } else {
                    //Auto-generate cohortId. Max CohortId + 1
                    cohortId = studyConfiguration.getCohortIds().size() > 0?
                            Collections.max(studyConfiguration.getCohortIds().values()) +1
                            : 0;    // if there are no cohorts and we are creating the first
                }
            } else {
                if (!cohortIds.containsKey(cohortName)) {
                    //ERROR Missing cohortId
                    throw new IOException("Missing cohortId for the cohort : " + cohortName);
                }
                cohortId = cohortIds.get(entry.getKey());
            }

            // check that the cohortId-cohortName is consistent with StudyConfiguration
            if (studyConfiguration.getCohortIds().containsKey(cohortName)) {
                if (!studyConfiguration.getCohortIds().get(cohortName).equals(cohortId)) {
                    //ERROR Duplicated cohortName
                    throw new IOException("Duplicated cohortName " + cohortName + ":" + cohortId + ". Appears in the StudyConfiguration as " +
                            cohortName + ":" + studyConfiguration.getCohortIds().get(cohortName));
                }
            } else if (studyConfiguration.getCohortIds().containsValue(cohortId)) {
                //ERROR Duplicated cohortId
                throw new IOException("Duplicated cohortId " + cohortName + ":" + cohortId + ". Appears in the StudyConfiguration as " +
                        StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).get(cohortId) + ":" + cohortId);
            }

            Set<Integer> sampleIds;
            if (samples == null) {
                //There are not provided samples for this cohort. Take samples from StudyConfiguration
                sampleIds = studyConfiguration.getCohorts().get(cohortId);
                if (sampleIds == null || (sampleIds.isEmpty()
                        && VariantSource.Aggregation.NONE.equals(studyConfiguration.getAggregation()))) {
                    //ERROR: StudyConfiguration does not have samples for this cohort, and it is not an aggregated study
                    throw new IOException("Cohort \"" + cohortName + "\" is empty");
                }
                samples = new HashSet<>();
                Map<Integer, String> idSamples = StudyConfiguration.inverseMap(studyConfiguration.getSampleIds());
                for (Integer sampleId : sampleIds) {
                    samples.add(idSamples.get(sampleId));
                }
                cohorts.put(cohortName, samples);
            } else if (samples.isEmpty()) {
                if (VariantSource.Aggregation.NONE.equals(studyConfiguration.getAggregation())) {  // if no aggregated study
                    throw new IOException("Cohort \"" + cohortName + "\" is empty");
                } else {
                    sampleIds = Collections.EMPTY_SET;
                }
            } else {
                sampleIds = new HashSet<>(samples.size());
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
                if (studyConfiguration.getCohorts().get(cohortId) != null && !sampleIds.equals(studyConfiguration.getCohorts().get(cohortId))) {
                    if (!studyConfiguration.getInvalidStats().contains(cohortId)) {
                        //If provided samples are different than the stored in the StudyConfiguration, and the cohort was not invalid.
                        throw new IOException("Different samples in cohort " + cohortName + ":" + cohortId + ". " +
                                "Samples in the StudyConfiguration: " + studyConfiguration.getCohorts().get(cohortId).size() + ". " +
                                "Samples provided " + samples.size() + ". Invalidate stats to continue.");
                    }
                }
            }

//            if (studyConfiguration.getInvalidStats().contains(cohortId)) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
//            }
            if (studyConfiguration.getCalculatedStats().contains(cohortId)) {
                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated");
            }

            cohortIdList.add(cohortId);
            studyConfiguration.getCohortIds().put(cohortName, cohortId);
            studyConfiguration.getCohorts().put(cohortId, sampleIds);
        }
        return cohortIdList;
    }

    void checkAndUpdateCalculatedCohorts(StudyConfiguration studyConfiguration, URI uri) throws IOException {

        /** Open input streams **/
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        /** Initialize Json parse **/
        try (JsonParser parser = jsonFactory.createParser(variantInputStream)) {
            if (parser.nextToken() != null) {
                VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);
                Set<String> cohortNames = variantStatsWrapper.getCohortStats().keySet();
                checkAndUpdateCalculatedCohorts(studyConfiguration, cohortNames);
            } else {
                throw new IOException("File " + uri + " is empty");
            }
        }
    }

    /**
     *
     *
     */
    void checkAndUpdateCalculatedCohorts(StudyConfiguration studyConfiguration, Collection<String> cohorts) throws IOException {
        for (String cohortName : cohorts) {
//            if (cohortName.equals(VariantSourceEntry.DEFAULT_COHORT)) {
//                continue;
//            }
            Integer cohortId = studyConfiguration.getCohortIds().get(cohortName);
            if (studyConfiguration.getInvalidStats().contains(cohortId)) {
//                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated and INVALID");
                logger.debug("Cohort \"" + cohortName + "\" stats calculated and INVALID. Set as calculated");
                studyConfiguration.getInvalidStats().remove(cohortId);
            }
            if (studyConfiguration.getCalculatedStats().contains(cohortId)) {
                throw new IOException("Cohort \"" + cohortName + "\" stats already calculated");
            } else {
                studyConfiguration.getCalculatedStats().add(cohortId);
            }
        }
    }

}

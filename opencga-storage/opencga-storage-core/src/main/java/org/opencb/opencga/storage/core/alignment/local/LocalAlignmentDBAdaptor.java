/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.alignment.local;

import ga4gh.Reads;
import htsjdk.samtools.SAMRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.biodata.tools.alignment.BamUtils;
import org.opencb.biodata.tools.alignment.filters.AlignmentFilters;
import org.opencb.biodata.tools.alignment.filters.SamRecordFilters;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.SamRecordAlignmentIterator;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 26/10/16.
 */
public class LocalAlignmentDBAdaptor implements AlignmentDBAdaptor {

    private int chunkSize;

    private static final int MINOR_CHUNK_SIZE = 1000;
    private static final int DEFAULT_CHUNK_SIZE = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 1000000;

    private static final String COVERAGE_SUFFIX = ".coverage";
    private static final String COVERAGE_DATABASE_NAME = "coverage.db";


    public LocalAlignmentDBAdaptor() {
        this(DEFAULT_CHUNK_SIZE);
    }

    public LocalAlignmentDBAdaptor(int chunkSize) {
        this.chunkSize = chunkSize;
    }


    @Override
    public QueryResult<ReadAlignment> get(Path path, Query query, QueryOptions options) {
        try {
            FileUtils.checkFile(path);

            StopWatch watch = StopWatch.createStarted();

            BamManager bamManager = new BamManager(path);
            Region region = parseRegion(query);
            AlignmentFilters<SAMRecord> alignmentFilters = parseQuery(query);
            AlignmentOptions alignmentOptions = parseQueryOptions(options);

            String queryResultId;
            List<ReadAlignment> readAlignmentList;
            if (region != null) {
                readAlignmentList = bamManager.query(region, alignmentFilters, alignmentOptions, ReadAlignment.class);
                queryResultId = region.toString();
            } else {
                readAlignmentList = bamManager.query(alignmentFilters, alignmentOptions, ReadAlignment.class);
                queryResultId = "Get alignments";
            }

            bamManager.close();
            watch.stop();
            return new QueryResult<>(queryResultId, ((int) watch.getTime()), readAlignmentList.size(), readAlignmentList.size(), null, null,
                    readAlignmentList);
        } catch (Exception e) {
            e.printStackTrace();
            return new QueryResult<>();
        }
    }

    @Override
    public ProtoAlignmentIterator iterator(Path path) {
        return iterator(path, new Query(), new QueryOptions());
    }

    @Override
    public ProtoAlignmentIterator iterator(Path path, Query query, QueryOptions options) {
        return (ProtoAlignmentIterator) iterator(path, query, options, Reads.ReadAlignment.class);
    }

    @Override
    public <T> AlignmentIterator<T> iterator(Path path, Query query, QueryOptions options, Class<T> clazz) {
        try {
            FileUtils.checkFile(path);

            BamManager bamManager = new BamManager(path);
            Region region = parseRegion(query);
            AlignmentFilters<SAMRecord> alignmentFilters = parseQuery(query);
            AlignmentOptions alignmentOptions = parseQueryOptions(options);

            if (region != null) {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(bamManager.iterator(region,
                            alignmentFilters, alignmentOptions, Reads.ReadAlignment.class));
                } else if (SAMRecord.class == clazz) {
                    return (AlignmentIterator<T>) new SamRecordAlignmentIterator(bamManager.iterator(region,
                            alignmentFilters, alignmentOptions, SAMRecord.class));
                }
            } else {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(bamManager.iterator(alignmentFilters,
                            alignmentOptions, Reads.ReadAlignment.class));
                } else if (SAMRecord.class == clazz) {
                    return (AlignmentIterator<T>) new SamRecordAlignmentIterator(bamManager.iterator(alignmentFilters,
                            alignmentOptions, SAMRecord.class));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Region region, int windowSize) throws Exception {
//        QueryOptions options = new QueryOptions();
//        options.put(QueryParams.WINDOW_SIZE.key(), DEFAULT_WINDOW_SIZE);
//        options.put(QueryParams.CONTAINED.key(), false);
//        return coverage(path, workspace, new Query(), options);
        FileUtils.checkFile(path);

        StopWatch watch = StopWatch.createStarted();
        RegionCoverage regionCoverage;
        if (path.toFile().getName().endsWith(".bam")) {
            BamManager bamManager = new BamManager(path);
            regionCoverage = bamManager.coverage(region, windowSize);
            bamManager.close();
        } else {
            regionCoverage = BamUtils.getCoverageFromBigWig(region, windowSize, path);
        }
        watch.stop();
        return new QueryResult<>(region.toString(), ((int) watch.getTime()), 1, 1, null, null, Collections.singletonList(regionCoverage));
    }

    @Override
    public QueryResult<RegionCoverage> getLowCoverageRegions(Path path, Region region, int maxCoverage) throws Exception {
        FileUtils.checkFile(path);

        StopWatch watch = StopWatch.createStarted();
        RegionCoverage regionCoverage;
        if (path.toFile().getName().endsWith(".bam")) {
            BamManager bamManager = new BamManager(path);
            regionCoverage = bamManager.coverage(region, 1);
            bamManager.close();
        } else {
            regionCoverage = BamUtils.getCoverageFromBigWig(region, 1, path);
        }
        List<RegionCoverage> regionCoverages = BamUtils.getUncoveredRegions(regionCoverage, maxCoverage);

        watch.stop();
        return new QueryResult<>(region.toString(), ((int) watch.getTime()), regionCoverages.size(), regionCoverages.size(),
                null, null, regionCoverages);
    }

    @Override
    public QueryResult<Long> count(Path path, Query query, QueryOptions options) {
        StopWatch watch = StopWatch.createStarted();

        ProtoAlignmentIterator iterator = iterator(path, query, options);
        long count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }

        watch.stop();
        return new QueryResult<>("Get count", (int) watch.getTime(), 1, 1, "", "", Collections.singletonList(count));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace) throws Exception {
        StopWatch watch = StopWatch.createStarted();

        FileUtils.checkFile(path);
        FileUtils.checkDirectory(workspace);

        Path statsPath = workspace.resolve(path.getFileName() + ".stats");
        AlignmentGlobalStats alignmentGlobalStats;

        if (statsPath.toFile().exists()) {
            // Read the file of stats
            ObjectMapper objectMapper = new ObjectMapper();
            alignmentGlobalStats = objectMapper.readValue(statsPath.toFile(), AlignmentGlobalStats.class);
        } else {
            BamManager alignmentManager = new BamManager(path);
            alignmentGlobalStats = alignmentManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.typedWriter(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), alignmentGlobalStats);
        }

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Collections.singletonList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        FileUtils.checkFile(path);

        StopWatch watch = StopWatch.createStarted();

        if (options == null) {
            options = new QueryOptions();
        }

        if (options.size() == 0 && query.size() == 0) {
            return stats(path, workspace);
        }

        Region region = parseRegion(query);
        AlignmentFilters alignmentFilters = parseQuery(query);
        AlignmentOptions alignmentOptions = parseQueryOptions(options);

        BamManager alignmentManager = new BamManager(path);
        AlignmentGlobalStats alignmentGlobalStats = alignmentManager.stats(region, alignmentFilters, alignmentOptions);

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }


//    @Override
//    public QueryResult<RegionCoverage> coverage(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
//        StopWatch watch = new StopWatch();
//        watch.start();
//
//        FileUtils.checkFile(path);
//
//        if (query == null) {
//            query = new Query();
//        }
//
//        if (options == null) {
//            options = new QueryOptions();
//        }
//
//        AlignmentOptions alignmentOptions = parseQueryOptions(options);
//        AlignmentFilters alignmentFilters = parseQuery(query);
//        Region region = parseRegion(query);
//
//        String queryResultId;
//        int windowSize;
//        RegionCoverage coverage = null;
//
//        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
//        if (!coverageDBPath.toFile().exists()
//                && (region == null || (region.getEnd() - region.getStart() > 50 * MINOR_CHUNK_SIZE))) {
//            createDBCoverage(path, workspace);
//        }
//
//        ChunkFrequencyManager chunkFrequencyManager = new ChunkFrequencyManager(coverageDBPath, chunkSize);
//        ChunkFrequencyManager.ChunkFrequency chunkFrequency = null;
//        if (region != null) {
//            if (region.getEnd() - region.getStart() > 50 * MINOR_CHUNK_SIZE) {
//                // if region is too big then we calculate the mean. We need to protect this code!
//                // and query SQLite database
//                windowSize = options.getInt(QueryParams.WINDOW_SIZE.key(), DEFAULT_WINDOW_SIZE);
//                chunkFrequency = chunkFrequencyManager.query(region, path, windowSize);
//            } else {
//                // if region is small enough we calculate all coverage for all positions dynamically
//                // calling the biodata alignment manager
//                BamManager alignmentManager = new BamManager(path);
//                coverage = alignmentManager.coverage(region, alignmentFilters, alignmentOptions);
//            }
//            queryResultId = region.toString();
//        } else {
//            // if no region is given we set up the windowSize to default value,
//            // we should return a few thousands mean values
//            // and query SQLite database
//            windowSize = DEFAULT_WINDOW_SIZE;
//            SAMFileHeader fileHeader = BamUtils.getFileHeader(path);
//            SAMSequenceRecord seq = fileHeader.getSequenceDictionary().getSequences().get(0);
//            int arraySize = Math.min(50 * MINOR_CHUNK_SIZE, seq.getSequenceLength());
//
//            region = new Region(seq.getSequenceName(), 1, arraySize * MINOR_CHUNK_SIZE);
//            queryResultId = "Get coverage";
//            chunkFrequency = chunkFrequencyManager.query(region, path, windowSize);
//        }
//
//        if (coverage == null && chunkFrequency != null) {
//            coverage = new RegionCoverage(region, chunkFrequency.getWindowSize(), chunkFrequency.getValues());
//        }
//
//        watch.stop();
//        return new QueryResult(queryResultId, ((int) watch.getTime()), 1, 1, null, null, Arrays.asList(coverage));
//    }

    private Region parseRegion(Query query) {
        Region region = null;
        if (query != null) {
            String regionString = query.getString(QueryParams.REGION.key());
            if (StringUtils.isNotEmpty(regionString)) {
                region = new Region(regionString);
            }
        }
        return region;
    }

    private AlignmentFilters<SAMRecord> parseQuery(Query query) {
        AlignmentFilters<SAMRecord> alignmentFilters = SamRecordFilters.create();

        if (query != null) {
            int minMapQ = query.getInt(QueryParams.MIN_MAPQ.key());
            if (minMapQ > 0) {
                alignmentFilters.addMappingQualityFilter(minMapQ);
            }

            int numMismatches = query.getInt(QueryParams.MAX_NM.key());
            if (numMismatches > 0) {
                alignmentFilters.addMaxNumberMismatchesFilter(numMismatches);
            }

            int numHits = query.getInt(QueryParams.MAX_NH.key());
            if (numHits > 0) {
                alignmentFilters.addMaxNumberHitsFilter(numHits);
            }

            if (query.getBoolean(QueryParams.PROPERLY_PAIRED.key())) {
                alignmentFilters.addProperlyPairedFilter();
            }

            int maxInsertSize = query.getInt(QueryParams.MAX_INSERT_SIZE.key());
            if (maxInsertSize > 0) {
                alignmentFilters.addInsertSizeFilter(maxInsertSize);
            }

            if (query.getBoolean(QueryParams.SKIP_UNMAPPED.key())) {
                alignmentFilters.addUnmappedFilter();
            }

            if (query.getBoolean(QueryParams.SKIP_DUPLICATED.key())) {
                alignmentFilters.addDuplicatedFilter();
            }
        }

        return alignmentFilters;
    }

    private AlignmentOptions parseQueryOptions(QueryOptions options) {
        AlignmentOptions alignmentOptions = new AlignmentOptions();

        if (options != null) {
            alignmentOptions.setContained(options.getBoolean(QueryParams.CONTAINED.key(), false));
            int windowSize = options.getInt(QueryParams.WINDOW_SIZE.key());
            if (windowSize > 0) {
                alignmentOptions.setWindowSize(windowSize);
            }

            int limit = options.getInt(QueryOptions.LIMIT);
            if (limit > 0) {
                alignmentOptions.setLimit(limit);
            }
        }

        return alignmentOptions;
    }

//    private void createDBCoverage(Path filePath, Path workspace) throws IOException {
//        SAMFileHeader fileHeader = BamUtils.getFileHeader(filePath);
//
//        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
//        ChunkFrequencyManager chunkFrequencyManager = new ChunkFrequencyManager(coverageDBPath);
//        List<String> chromosomeNames = new ArrayList<>();
//        List<Integer> chromosomeLengths = new ArrayList<>();
//        fileHeader.getSequenceDictionary().getSequences().forEach(
//                seq -> {
//                    chromosomeNames.add(seq.getSequenceName());
//                    chromosomeLengths.add(seq.getSequenceLength());
//                });
//        chunkFrequencyManager.init(chromosomeNames, chromosomeLengths);
//
//        Path coveragePath = workspace.toAbsolutePath().resolve(filePath.getFileName() + COVERAGE_SUFFIX);
//
//        AlignmentOptions options = new AlignmentOptions();
//        options.setContained(false);
//
//        BamUtils.createCoverageWigFile(filePath, coveragePath, 200);
//        chunkFrequencyManager.loadWigFile(coveragePath, filePath);
//    }

}

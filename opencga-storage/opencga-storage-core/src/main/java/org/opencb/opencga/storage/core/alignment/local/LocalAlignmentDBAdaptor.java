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
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMSequenceRecord;
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
import org.opencb.biodata.tools.commons.ChunkFrequencyManager;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;
import org.opencb.opencga.storage.core.alignment.iterators.SamRecordAlignmentIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
    public QueryResult getAllAlignmentsByRegion(List<Region> regions, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAllAlignmentsByGene(String gene, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getCoverageByRegion(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAlignmentsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAllIntervalFrequencies(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult getAlignmentRegionInfo(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryResult<ReadAlignment> get(Path path, Query query, QueryOptions options) {
        try {
            StopWatch watch = new StopWatch();
            watch.start();

            FileUtils.checkFile(path);
            BamManager alignmentManager = new BamManager(path);

            AlignmentOptions alignmentOptions = parseQueryOptions(options);
            AlignmentFilters<SAMRecord> alignmentFilters = parseQuery(query);
            Region region = parseRegion(query);

            String queryResultId;
            List<ReadAlignment> readAlignmentList;
            if (region != null) {
                readAlignmentList = alignmentManager.query(region, alignmentFilters, alignmentOptions, ReadAlignment.class);
                queryResultId = region.toString();
            } else {
                readAlignmentList = alignmentManager.query(alignmentFilters, alignmentOptions, ReadAlignment.class);
                queryResultId = "Get alignments";
            }
//            List<String> stringFormatList = new ArrayList<>(readAlignmentList.size());
//            for (Reads.ReadAlignment readAlignment : readAlignmentList) {
//                stringFormatList.add(readAlignment());
//            }
//            List<JsonFormat> list = alignmentManager.query(region, alignmentOptions, alignmentFilters, Reads.ReadAlignment.class);
            watch.stop();
            return new QueryResult(queryResultId, ((int) watch.getTime()), readAlignmentList.size(), readAlignmentList.size(), null, null,
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

            if (query == null) {
                query = new Query();
            }

            if (options == null) {
                options = new QueryOptions();
            }

            BamManager alignmentManager = new BamManager(path);
            AlignmentFilters<SAMRecord> alignmentFilters = parseQuery(query);
            AlignmentOptions alignmentOptions = parseQueryOptions(options);

            Region region = parseRegion(query);
            if (region != null) {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(alignmentManager.iterator(region,
                            alignmentFilters, alignmentOptions, Reads.ReadAlignment.class));
                } else if (SAMRecord.class == clazz) {
                    return (AlignmentIterator<T>) new SamRecordAlignmentIterator(alignmentManager.iterator(region,
                            alignmentFilters, alignmentOptions, SAMRecord.class));
                }
            } else {
                if (Reads.ReadAlignment.class == clazz) {
                    return (AlignmentIterator<T>) new ProtoAlignmentIterator(alignmentManager.iterator(alignmentFilters,
                            alignmentOptions, Reads.ReadAlignment.class));
                } else if (SAMRecord.class == clazz) {
                    return (AlignmentIterator<T>) new SamRecordAlignmentIterator(alignmentManager.iterator(alignmentFilters,
                            alignmentOptions, SAMRecord.class));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public QueryResult<Long> count(Path path, Query query, QueryOptions options) {
        StopWatch watch = new StopWatch();
        watch.start();

        ProtoAlignmentIterator iterator = iterator(path, query, options);
        long cont = 0;
        while (iterator.hasNext()) {
            iterator.next();
            cont++;
        }

        watch.stop();
        return new QueryResult<>("Get count", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(cont));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

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
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<AlignmentGlobalStats> stats(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

        if (options.size() == 0 && query.size() == 0) {
            return stats(path, workspace);
        }

        FileUtils.checkFile(path);

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        BamManager alignmentManager = new BamManager(path);
        AlignmentGlobalStats alignmentGlobalStats = alignmentManager.stats(region, alignmentFilters, alignmentOptions);

        watch.stop();
        return new QueryResult<>("Get stats", (int) watch.getTime(), 1, 1, "", "", Arrays.asList(alignmentGlobalStats));
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Path workspace) throws Exception {
        QueryOptions options = new QueryOptions();
        options.put(QueryParams.WINDOW_SIZE.key(), DEFAULT_WINDOW_SIZE);
        options.put(QueryParams.CONTAINED.key(), false);
        return coverage(path, workspace, new Query(), options);
    }

    @Override
    public QueryResult<RegionCoverage> coverage(Path path, Path workspace, Query query, QueryOptions options) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        FileUtils.checkFile(path);

        if (query == null) {
            query = new Query();
        }

        if (options == null) {
            options = new QueryOptions();
        }

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        String queryResultId;
        int windowSize;
        RegionCoverage coverage = null;

        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
        if (!coverageDBPath.toFile().exists()
                && (region == null || (region.getEnd() - region.getStart() > 50 * MINOR_CHUNK_SIZE))) {
            createDBCoverage(path, workspace);
        }

        ChunkFrequencyManager chunkFrequencyManager = new ChunkFrequencyManager(coverageDBPath, chunkSize);
        ChunkFrequencyManager.ChunkFrequency chunkFrequency = null;
        if (region != null) {
            if (region.getEnd() - region.getStart() > 50 * MINOR_CHUNK_SIZE) {
                // if region is too big then we calculate the mean. We need to protect this code!
                // and query SQLite database
                windowSize = options.getInt(QueryParams.WINDOW_SIZE.key(), DEFAULT_WINDOW_SIZE);
                chunkFrequency = chunkFrequencyManager.query(region, path, windowSize);
            } else {
                // if region is small enough we calculate all coverage for all positions dynamically
                // calling the biodata alignment manager
                BamManager alignmentManager = new BamManager(path);
                coverage = alignmentManager.coverage(region, alignmentFilters, alignmentOptions);
            }
            queryResultId = region.toString();
        } else {
            // if no region is given we set up the windowSize to default value,
            // we should return a few thousands mean values
            // and query SQLite database
            windowSize = DEFAULT_WINDOW_SIZE;
            SAMFileHeader fileHeader = BamUtils.getFileHeader(path);
            SAMSequenceRecord seq = fileHeader.getSequenceDictionary().getSequences().get(0);
            int arraySize = Math.min(50 * MINOR_CHUNK_SIZE, seq.getSequenceLength());

            region = new Region(seq.getSequenceName(), 1, arraySize * MINOR_CHUNK_SIZE);
            queryResultId = "Get coverage";
            chunkFrequency = chunkFrequencyManager.query(region, path, windowSize);
        }

        if (coverage == null) {
            coverage = new RegionCoverage(region, chunkFrequency.getWindowSize(), chunkFrequency.getValues());
        }

        watch.stop();
        return new QueryResult(queryResultId, ((int) watch.getTime()), 1, 1, null, null, Arrays.asList(coverage));
    }

    private Region parseRegion(Query query) {
        Region region = null;
        String regionString = query.getString(QueryParams.REGION.key());
        if (regionString != null && !regionString.isEmpty()) {
            region = new Region(regionString);
        }
        return region;
    }

    private AlignmentFilters<SAMRecord> parseQuery(Query query) {
        AlignmentFilters<SAMRecord> alignmentFilters = SamRecordFilters.create();
        int minMapQ = query.getInt(QueryParams.MIN_MAPQ.key());
        if (minMapQ > 0) {
            alignmentFilters.addMappingQualityFilter(minMapQ);
        }
        return  alignmentFilters;
    }

    private AlignmentOptions parseQueryOptions(QueryOptions options) {
        AlignmentOptions alignmentOptions = new AlignmentOptions()
                .setContained(options.getBoolean(QueryParams.CONTAINED.key()));
        int windowSize = options.getInt(QueryParams.WINDOW_SIZE.key());
        if (windowSize > 0) {
            alignmentOptions.setWindowSize(windowSize);
        }
        int limit = options.getInt(QueryParams.LIMIT.key());
        if (limit > 0) {
            alignmentOptions.setLimit(limit);
        }
        return alignmentOptions;
    }

    private void createDBCoverage(Path filePath, Path workspace) throws IOException {
        SAMFileHeader fileHeader = BamUtils.getFileHeader(filePath);

        Path coverageDBPath = workspace.toAbsolutePath().resolve(COVERAGE_DATABASE_NAME);
        ChunkFrequencyManager chunkFrequencyManager = new ChunkFrequencyManager(coverageDBPath);
        List<String> chromosomeNames = new ArrayList<>();
        List<Integer> chromosomeLengths = new ArrayList<>();
        fileHeader.getSequenceDictionary().getSequences().forEach(
                seq -> {
                    chromosomeNames.add(seq.getSequenceName());
                    chromosomeLengths.add(seq.getSequenceLength());
                });
        chunkFrequencyManager.init(chromosomeNames, chromosomeLengths);

        Path coveragePath = workspace.toAbsolutePath().resolve(filePath.getFileName() + COVERAGE_SUFFIX);

        AlignmentOptions options = new AlignmentOptions();
        options.setContained(false);

        BamUtils.createCoverageWigFile(filePath, coveragePath, 200);
        chunkFrequencyManager.loadWigFile(coveragePath, filePath);
    }

}

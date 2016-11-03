package org.opencb.opencga.storage.core.alignment.local;

import ga4gh.Reads;
import org.apache.commons.lang3.time.StopWatch;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentFilters;
import org.opencb.biodata.tools.alignment.AlignmentManager;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptor implements AlignmentDBAdaptor {

    private int chunkSize;

    private static final int DEFAULT_CHUNK_SIZE = 1000;
    private static final int DEFAULT_WINDOW_SIZE = 1_000_000;

    DefaultAlignmentDBAdaptor() {
        this(DEFAULT_CHUNK_SIZE);
    }

    public DefaultAlignmentDBAdaptor(int chunkSize) {
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
    public QueryResult<ReadAlignment> get(String fileId, Query query, QueryOptions options) {
        try {
            StopWatch watch = new StopWatch();
            watch.start();

            Path path = Paths.get(fileId);
            FileUtils.checkFile(path);
            AlignmentManager alignmentManager = new AlignmentManager(path);

            AlignmentOptions alignmentOptions = parseQueryOptions(options);
            AlignmentFilters alignmentFilters = parseQuery(query);
            Region region = parseRegion(query);

            List<ReadAlignment> readAlignmentList;
            if (region != null) {
                readAlignmentList = alignmentManager.query(region, alignmentOptions, alignmentFilters, ReadAlignment.class);
            } else {
                readAlignmentList = alignmentManager.query(alignmentOptions, alignmentFilters, ReadAlignment.class);
            }
//            List<String> stringFormatList = new ArrayList<>(readAlignmentList.size());
//            for (Reads.ReadAlignment readAlignment : readAlignmentList) {
//                stringFormatList.add(readAlignment());
//            }
//            List<JsonFormat> list = alignmentManager.query(region, alignmentOptions, alignmentFilters, Reads.ReadAlignment.class);
            watch.stop();
            return new QueryResult("Get alignments", ((int) watch.getTime()), readAlignmentList.size(), readAlignmentList.size(),
                    null, null, readAlignmentList);
        } catch (Exception e) {
            e.printStackTrace();
            return new QueryResult<>();
        }
    }

    @Override
    public ProtoAlignmentIterator iterator(String fileId) {
        return iterator(fileId, new Query(), new QueryOptions());
    }

    @Override
    public ProtoAlignmentIterator iterator(String fileId, Query query, QueryOptions options) {
        try {
            Path path = Paths.get(fileId);
            FileUtils.checkFile(path);

            if (query == null) {
                query = new Query();
            }

            if (options == null) {
                options = new QueryOptions();
            }

            AlignmentManager alignmentManager = new AlignmentManager(path);
            AlignmentFilters alignmentFilters = parseQuery(query);
            AlignmentOptions alignmentOptions = parseQueryOptions(options);

            Region region = parseRegion(query);
            if (region != null) {
                return new ProtoAlignmentIterator(alignmentManager.iterator(region, alignmentOptions, alignmentFilters,
                        Reads.ReadAlignment.class));
            } else {
                return new ProtoAlignmentIterator(alignmentManager.iterator(alignmentOptions, alignmentFilters, Reads.ReadAlignment.class));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public long count(String fileId, Query query, QueryOptions options) {
        ProtoAlignmentIterator iterator = iterator(fileId, query, options);
        long cont = 0;
        while (iterator.hasNext()) {
            iterator.next();
            cont++;
        }
        return cont;
    }

    @Override
    public AlignmentGlobalStats stats(String fileId) throws Exception {
        Path path = Paths.get(fileId);
        Path statsPath = path.getParent().resolve(path.getFileName() + ".stats");
        AlignmentGlobalStats alignmentGlobalStats;
        if (statsPath.toFile().exists()) {
            // Read the file of stats
            ObjectMapper objectMapper = new ObjectMapper();
            alignmentGlobalStats = objectMapper.readValue(statsPath.toFile(), AlignmentGlobalStats.class);
        } else {
            AlignmentManager alignmentManager = new AlignmentManager(path);
            alignmentGlobalStats = alignmentManager.stats();
            ObjectMapper objectMapper = new ObjectMapper();
            ObjectWriter objectWriter = objectMapper.typedWriter(AlignmentGlobalStats.class);
            objectWriter.writeValue(statsPath.toFile(), alignmentGlobalStats);
        }

        return alignmentGlobalStats;
    }

    @Override
    public AlignmentGlobalStats stats(String fileId, Query query, QueryOptions options) throws Exception {
        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

        if (options.size() == 0 && query.size() == 0) {
            return stats(fileId);
        }

        Path path = Paths.get(fileId);
        FileUtils.checkFile(path);

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        AlignmentManager alignmentManager = new AlignmentManager(path);

        return alignmentManager.stats(region, alignmentOptions, alignmentFilters);
    }

    @Override
    public RegionCoverage coverage(String fileId) throws Exception {
        return coverage(fileId, new Query(), new QueryOptions("windowSize", DEFAULT_WINDOW_SIZE));
    }

    @Override
    public RegionCoverage coverage(String fileId, Query query, QueryOptions options) throws Exception {
        Path path = Paths.get(fileId);
        FileUtils.checkFile(path);

        if (query == null) {
            query = new Query();
        }

        if (options == null) {
            options = new QueryOptions();
        }


        int windowSize;
        if (query.containsKey(QueryParams.REGION.key())) {
            Region region = Region.parseRegion(query.getString(QueryParams.REGION.key()));

            if (region.getEnd() - region.getStart() > 50 * DEFAULT_CHUNK_SIZE) {
                // if region is too big then we calculate the mean. We need to protect this code!
                windowSize = options.getInt("windowSize", DEFAULT_WINDOW_SIZE);
                // query SQLite
                // ...
            } else {
                // if regon is small enough we calculate all coverage for all positions dynamically
                windowSize = 1;
                // call to biodata...
                // ...
            }

        } else {
            // if no region is given we set up the windowSize to default value, we should return a few thousands mean values
            windowSize = DEFAULT_WINDOW_SIZE;
            // query SQLite
            // ...
        }

        return null;
    }

    private Region parseRegion(Query query) {
        Region region = null;
        String regionString = query.getString(QueryParams.REGION.key());
        if (regionString != null && !regionString.isEmpty()) {
            region = new Region(regionString);
        }
        return region;
    }

    private AlignmentFilters parseQuery(Query query) {
        AlignmentFilters alignmentFilters = AlignmentFilters.create();
        int minMapQ = query.getInt(QueryParams.MIN_MAPQ.key());
        if (minMapQ > 0) {
            alignmentFilters.addMappingQualityFilter(minMapQ);
        }
        return  alignmentFilters;
    }

    private AlignmentOptions parseQueryOptions(QueryOptions options) {
        AlignmentOptions alignmentOptions = new AlignmentOptions()
                .setContained(options.getBoolean(QueryParams.CONTAINED.key()));
        int limit = options.getInt(QueryParams.LIMIT.key());
        if (limit > 0) {
            alignmentOptions.setLimit(limit);
        }
        return alignmentOptions;
    }


    public int getChunkSize() {
        return chunkSize;
    }

    public DefaultAlignmentDBAdaptor setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }
}

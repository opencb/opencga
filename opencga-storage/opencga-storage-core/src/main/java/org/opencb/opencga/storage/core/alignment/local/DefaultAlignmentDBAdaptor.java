package org.opencb.opencga.storage.core.alignment.local;

import ga4gh.Reads;
import org.apache.commons.lang3.time.StopWatch;
import org.ga4gh.models.ReadAlignment;
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

    public DefaultAlignmentDBAdaptor() {
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
    public QueryResult get(String fileId, Query query, QueryOptions options) {
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
            if (options == null) {
                options = new QueryOptions();
            }
            if (query == null) {
                query = new Query();
            }

            Path path = Paths.get(fileId);
            FileUtils.checkFile(path);
            AlignmentManager alignmentManager = new AlignmentManager(path);

            AlignmentOptions alignmentOptions = parseQueryOptions(options);
            AlignmentFilters alignmentFilters = parseQuery(query);
            Region region = parseRegion(query);

            if (region != null) {
                return new ProtoAlignmentIterator(
                        alignmentManager.iterator(region, alignmentOptions, alignmentFilters, Reads.ReadAlignment.class));
            } else {
                return new ProtoAlignmentIterator(alignmentManager.iterator(alignmentOptions, alignmentFilters, Reads.ReadAlignment.class));
            }


        } catch (Exception e) {
            e.printStackTrace();
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
        FileUtils.checkFile(path);
        // TODO: This is wrong ! Go to the database or file
        AlignmentManager alignmentManager = new AlignmentManager(path);
        return alignmentManager.stats();
    }

    @Override
    public AlignmentGlobalStats stats(String fileId, Query query, QueryOptions options) throws Exception {
        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

        Path path = Paths.get(fileId);
        FileUtils.checkFile(path);

        AlignmentOptions alignmentOptions = parseQueryOptions(options);
        AlignmentFilters alignmentFilters = parseQuery(query);
        Region region = parseRegion(query);

        // TODO: Check if it is already in catalog
        AlignmentManager alignmentManager = new AlignmentManager(path);

        return alignmentManager.stats(region, alignmentOptions, alignmentFilters);
    }

}

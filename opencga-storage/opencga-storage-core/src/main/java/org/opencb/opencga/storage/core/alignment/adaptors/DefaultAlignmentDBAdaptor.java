package org.opencb.opencga.storage.core.alignment.adaptors;

import ga4gh.Reads;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.AlignmentManager;
import org.opencb.biodata.tools.alignment.AlignmentOptions;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.alignment.iterators.ProtoAlignmentIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptor implements AlignmentDBAdaptor {

    private AlignmentManager alignmentManager;

    public DefaultAlignmentDBAdaptor(Path input) throws IOException {
        alignmentManager = new AlignmentManager(input);
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
    public ProtoAlignmentIterator iterator() {
        return iterator(new Query(), new QueryOptions());
    }

    @Override
    public ProtoAlignmentIterator iterator(Query query, QueryOptions options) {
        if (options == null) {
            options = new QueryOptions();
        }
        if (query == null) {
            query = new Query();
        }

        // TODO: Parse query
        return new ProtoAlignmentIterator(
                alignmentManager.iterator("20", 60000, 65000, new AlignmentOptions(), null, Reads.ReadAlignment.class));
//
//        Document mongoQuery = parseQuery(query);
//        Document projection = createProjection(query, options);
//        DocumentToVariantConverter converter = getDocumentToVariantConverter(query, options);
//        options.putIfAbsent(MongoDBCollection.BATCH_SIZE, 100);
//
//        return new ProtoAlignmentIterator(alignmentManager.iterator(....));
//        // Short unsorted queries with timeout or limit don't need the persistent cursor.
//        if (options.containsKey(QueryOptions.TIMEOUT)
//                || options.containsKey(QueryOptions.LIMIT)
//                || !options.containsKey(QueryOptions.SORT)) {
//            FindIterable<Document> dbCursor = variantsCollection.nativeQuery().find(mongoQuery, projection, options);
//
//            return new VariantMongoDBIterator(dbCursor, converter);
//        } else {
//            return VariantMongoDBIterator.persistentIterator(variantsCollection, mongoQuery, projection, options, converter);
//        }
    }

}

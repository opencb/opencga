package org.opencb.opencga.storage.hadoop.variant.adaptors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created on 08/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSourceDBAdaptor implements VariantSourceDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(HadoopVariantSourceDBAdaptor.class);
    private final Connection connection;
    private final Configuration configuration;

    public HadoopVariantSourceDBAdaptor(Connection connection, Configuration configuration) {
        this.connection = connection;
        this.configuration = configuration;
    }

    @Override
    public QueryResult<Long> count() {
        logger.warn("Unimplemented method!");
        return null;
    }

    @Override
    public void updateVariantSource(VariantSource variantSource) {
        logger.warn("Unimplemented method!");
    }

    @Override
    public Iterator<VariantSource> iterator(Query query, QueryOptions options) {
        logger.warn("Unimplemented method!");
        return Collections.emptyIterator();
    }

    @Override
    public QueryResult updateSourceStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions queryOptions) {

        logger.warn("Unimplemented method!");

        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

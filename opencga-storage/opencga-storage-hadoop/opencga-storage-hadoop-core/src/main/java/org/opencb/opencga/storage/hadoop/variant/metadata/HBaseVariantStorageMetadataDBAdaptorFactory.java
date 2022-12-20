package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantTableHelper;

import java.io.IOException;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseVariantStorageMetadataDBAdaptorFactory implements VariantStorageMetadataDBAdaptorFactory {

    private final String metaTableName;
    private final Configuration configuration;
    private final HBaseManager hBaseManager;

    public HBaseVariantStorageMetadataDBAdaptorFactory(VariantTableHelper helper) {
        configuration = helper.getConf();
        metaTableName = helper.getMetaTableAsString();
        hBaseManager = new HBaseManager(configuration);
    }

    public HBaseVariantStorageMetadataDBAdaptorFactory(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        this.metaTableName = metaTableName;
        this.configuration = configuration;
        this.hBaseManager = hBaseManager;
    }

    @Override
    public ObjectMap getConfiguration() {
        ObjectMap objectMap = new ObjectMap();
        configuration.iterator().forEachRemaining(e -> objectMap.put(e.getKey(), e.getValue()));
        return objectMap;
    }

    @Override
    public HBaseFileMetadataDBAdaptor buildFileMetadataDBAdaptor() {
        return new HBaseFileMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseProjectMetadataDBAdaptor buildProjectMetadataDBAdaptor() {
        return new HBaseProjectMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseStudyMetadataDBAdaptor buildStudyMetadataDBAdaptor() {
        return new HBaseStudyMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseSampleMetadataDBAdaptor buildSampleMetadataDBAdaptor() {
        return new HBaseSampleMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseCohortMetadataDBAdaptor buildCohortMetadataDBAdaptor() {
        return new HBaseCohortMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseTaskMetadataDBAdaptor buildTaskDBAdaptor() {
        return new HBaseTaskMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public void close() throws IOException {
        this.hBaseManager.close();
    }
}

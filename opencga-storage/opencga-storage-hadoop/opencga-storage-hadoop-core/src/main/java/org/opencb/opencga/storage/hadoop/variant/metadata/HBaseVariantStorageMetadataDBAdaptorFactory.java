package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

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
        hBaseManager = null;
    }

    public HBaseVariantStorageMetadataDBAdaptorFactory(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        this.metaTableName = metaTableName;
        this.configuration = configuration;
        this.hBaseManager = hBaseManager;
    }

    @Override
    public HBaseVariantFileMetadataDBAdaptor buildVariantFileMetadataDBAdaptor() {
        return new HBaseVariantFileMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseProjectMetadataDBAdaptor buildProjectMetadataDBAdaptor() {
        return new HBaseProjectMetadataDBAdaptor(hBaseManager, metaTableName, configuration);
    }

    @Override
    public HBaseStudyConfigurationDBAdaptor buildStudyConfigurationDBAdaptor() {
        return new HBaseStudyConfigurationDBAdaptor(hBaseManager, metaTableName, configuration);
    }
}

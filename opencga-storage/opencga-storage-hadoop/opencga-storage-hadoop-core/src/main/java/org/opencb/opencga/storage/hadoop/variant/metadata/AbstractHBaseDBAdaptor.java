package org.opencb.opencga.storage.hadoop.variant.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantMetadataUtils.createMetaTableIfNeeded;

/**
 * Created on 03/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AbstractHBaseDBAdaptor {

    protected static Logger logger = LoggerFactory.getLogger(AbstractHBaseDBAdaptor.class);

    protected final HBaseManager hBaseManager;
    protected final ObjectMapper objectMapper;
    protected final String tableName;
    protected Boolean tableExists = null; // unknown
    protected byte[] family;

    public AbstractHBaseDBAdaptor(VariantTableHelper helper) {
        this(null, helper.getMetaTableAsString(), helper.getConf());
    }

    public AbstractHBaseDBAdaptor(HBaseManager hBaseManager, String metaTableName, Configuration configuration) {
        Objects.requireNonNull(configuration);
        this.tableName = Objects.requireNonNull(metaTableName);
        HBaseVariantTableNameGenerator.checkValidMetaTableName(metaTableName);
        family = new GenomeHelper(configuration).getColumnFamily();
        this.objectMapper = new ObjectMapper().addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        if (hBaseManager == null) {
            this.hBaseManager = new HBaseManager(configuration);
        } else {
            // Create a new instance of HBaseManager to close only if needed
            this.hBaseManager = new HBaseManager(hBaseManager);
        }
    }

    protected void ensureTableExists() throws IOException {
        if (tableExists == null || !tableExists) {
            if (createMetaTableIfNeeded(hBaseManager, tableName, family)) {
                logger.info("Create table '{}' in hbase!", tableName);
            }
            tableExists = true;
        }
    }
}

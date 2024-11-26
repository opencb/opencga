package org.opencb.opencga.storage.hadoop.variant.executors;

import org.apache.hadoop.conf.Configuration;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.MR_EXECUTOR;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public final class MRExecutorFactory {

    private static Logger logger = LoggerFactory.getLogger(SshMRExecutor.class);

    private MRExecutorFactory() {
    }

    public static MRExecutor getMRExecutor(String dbName, ObjectMap options, Configuration conf) throws StorageEngineException {
        MRExecutor mrExecutor;
        String executor = options.getString(MR_EXECUTOR.key(), MR_EXECUTOR.defaultValue());
        switch (executor.toLowerCase()) {
            case "system":
                mrExecutor = new SystemMRExecutor();
                break;
            case "ssh":
                mrExecutor = new SshMRExecutor();
                break;
            default:
                try {
                    logger.info("Creating new instance of MRExecutor '{}'", executor);
                    Class<? extends MRExecutor> aClass;
                    aClass = Class.forName(executor).asSubclass(MRExecutor.class);
                    mrExecutor = aClass.newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
                    throw new StorageEngineException("Error creating MRExecutor '" + executor + "'", e);
                }
                break;
        }

        // configure MRExecutor
        mrExecutor.init(dbName, conf, options);

        return mrExecutor;
    }

}

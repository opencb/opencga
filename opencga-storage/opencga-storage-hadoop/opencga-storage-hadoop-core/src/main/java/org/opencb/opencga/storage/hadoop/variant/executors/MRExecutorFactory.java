package org.opencb.opencga.storage.hadoop.variant.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngineOptions.MR_EXECUTOR;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MRExecutorFactory {

    public static MRExecutor getMRExecutor(ObjectMap options) throws StorageEngineException {
        MRExecutor mrExecutor;
        Class<? extends MRExecutor> aClass;
        String executor = options.getString(MR_EXECUTOR.key(), MR_EXECUTOR.defaultValue());
        switch (executor.toLowerCase()) {
            case "system":
                aClass = SystemMRExecutor.class;
                break;
            case "ssh":
                aClass = SshMRExecutor.class;
                break;
            default:
                try {
                    aClass = Class.forName(executor).asSubclass(MRExecutor.class);
                } catch (ClassNotFoundException | ClassCastException e) {
                    throw new StorageEngineException("Error creating MRExecutor '" + executor + "'", e);
                }
                break;
        }
        try {
            mrExecutor = aClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new StorageEngineException("Error creating MRExecutor '" + executor + "'", e);
        }

        // configure MRExecutor
        mrExecutor.init(options);

        return mrExecutor;
    }

}

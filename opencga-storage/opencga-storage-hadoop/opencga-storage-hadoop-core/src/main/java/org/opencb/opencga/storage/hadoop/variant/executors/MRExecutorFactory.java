package org.opencb.opencga.storage.hadoop.variant.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MRExecutorFactory {

    public static final String MR_EXECUTOR = "opencga.mr.executor";

    public static MRExecutor getMRExecutor(ObjectMap options) throws StorageEngineException {
        MRExecutor mrExecutor;
        if (options.containsKey(MR_EXECUTOR)) {
            Class<? extends MRExecutor> aClass;
            if (options.get(MR_EXECUTOR) instanceof Class) {
                aClass = ((Class<?>) options.get(MR_EXECUTOR, Class.class)).asSubclass(MRExecutor.class);
            } else {
                String className = options.getString(MR_EXECUTOR);
                switch (className.toLowerCase()) {
                    case "system":
                        aClass = SystemMRExecutor.class;
                        break;
                    case "ssh":
                        aClass = SshMRExecutor.class;
                        break;
                    default:
                        try {
                            aClass = Class.forName(className).asSubclass(MRExecutor.class);
                        } catch (ClassNotFoundException e) {
                            throw new StorageEngineException("Error creating MRExecutor", e);
                        }
                        break;
                }
            }
            try {
                mrExecutor = aClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new StorageEngineException("Error creating MRExecutor", e);
            }
        } else {
            mrExecutor = new SystemMRExecutor();
        }

        // configure MRExecutor
        mrExecutor.init(options);

        return mrExecutor;
    }

}

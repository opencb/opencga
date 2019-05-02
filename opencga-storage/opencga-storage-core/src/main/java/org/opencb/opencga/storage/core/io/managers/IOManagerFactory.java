package org.opencb.opencga.storage.core.io.managers;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IOManagerFactory {

    private List<IOManager> ioManagers;


    public IOManagerFactory() {
        ioManagers = new ArrayList<>(2);
        ioManagers.add(new AzureBlobStorageIOManager());
        ioManagers.add(new PosixIOManager());
    }

    public IOManagerFactory(List<String> ioManagersClazz) {
        ioManagers = new ArrayList<>(ioManagersClazz.size());
        List<Class<? extends IOManager>> classes = new ArrayList<>(ioManagersClazz.size());
        try {
            for (String ioManagerClazz : ioManagersClazz) {
                Class<?> aClass = Class.forName(ioManagerClazz);
                if (IOManager.class.isAssignableFrom(aClass)) {
                    classes.add((Class<? extends IOManager>) aClass);
                } else {
                    throw new IllegalArgumentException("Class " + aClass + " is not an instance of " + IOManager.class);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }

        try {
            for (Class<? extends IOManager> aClass : classes) {
                ioManagers.add(aClass.newInstance());
            }
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public IOManager get(URI uri) throws IOException {
        for (IOManager ioManager : ioManagers) {
            if (ioManager.supports(uri)) {
                return ioManager;
            }
        }
        throw new IOException("Unsupported file: " + uri);
    }

}

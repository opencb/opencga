package org.opencb.opencga.storage.core.io.managers;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.IOManagersConfiguration;
import org.opencb.opencga.storage.core.config.IOManagersConfiguration.IOManagerConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IOManagerProvider implements IOManager {

    private final List<IOManager> ioManagers;

    public IOManagerProvider(StorageConfiguration configuration) {
        IOManagersConfiguration io = configuration.getIo();
        if (io == null) {
            io = new IOManagersConfiguration(Collections.singletonMap(
                    "local", new IOManagerConfiguration(LocalIOManager.class.getName(), null)));
        }
        ioManagers = new ArrayList<>(io.getManagers().size());

        for (IOManagerConfiguration value : io.getManagers().values()) {
            Class<? extends IOManager> aClass = getClass(value.getClazz());
            ioManagers.add(newInstance(aClass, value.getOptions()));
        }
    }

    public IOManagerProvider(Class<? extends IOManager>... classes) {
        this(Arrays.stream(classes).map(Class::getName).collect(Collectors.toList()));
    }

    public IOManagerProvider(List<String> ioManagersClassName) {
        ioManagers = new ArrayList<>(ioManagersClassName.size());
        for (String ioManagerClazz : ioManagersClassName) {
            ioManagers.add(newInstance(getClass(ioManagerClazz), null));
        }
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends IOManager> getClass(String ioManagerClazz) {
        Class<?> aClass;
        try {
            aClass = Class.forName(ioManagerClazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
        if (IOManager.class.isAssignableFrom(aClass)) {
            return (Class<? extends IOManager>) aClass;
        } else {
            throw new IllegalArgumentException("Class " + aClass + " is not an instance of " + IOManager.class);
        }
    }

    protected IOManager newInstance(Class<? extends IOManager> aClass, ObjectMap options) {
        try {
            if (options != null) {
                for (Constructor<?> constructor : aClass.getConstructors()) {
                    if (constructor.getParameterCount() == 1
                            && constructor.getParameters()[0].getType().isAssignableFrom(ObjectMap.class)) {
                        return (IOManager) constructor.newInstance(options);
                    }
                }
            }
            return aClass.newInstance();
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public synchronized void add(IOManager ioManager) {
        if (!ioManagers.contains(ioManager)) {
            ioManagers.add(ioManager);
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

    @Override
    public boolean supports(URI uri) {
        try {
            return get(uri) != null;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public InputStream newInputStreamRaw(URI uri) throws IOException {
        return get(uri).newInputStreamRaw(uri);
    }

    @Override
    public InputStream newInputStream(URI uri) throws IOException {
        return get(uri).newInputStream(uri);
    }

    @Override
    public OutputStream newOutputStreamRaw(URI uri) throws IOException {
        return get(uri).newOutputStreamRaw(uri);
    }

    @Override
    public OutputStream newOutputStream(URI uri) throws IOException {
        return get(uri).newOutputStream(uri);
    }

    @Override
    public boolean exists(URI uri) throws IOException {
        return get(uri).exists(uri);
    }

    @Override
    public boolean isDirectory(URI uri) throws IOException {
        return get(uri).isDirectory(uri);
    }

    @Override
    public boolean canWrite(URI uri) throws IOException {
        return get(uri).canWrite(uri);
    }

    @Override
    public void checkWritable(URI uri) throws IOException {
        get(uri).checkWritable(uri);
    }

    @Override
    public void copyFromLocal(Path localSourceFile, URI targetFile) throws IOException {
        get(targetFile).copyFromLocal(localSourceFile, targetFile);
    }

    @Override
    public void copyToLocal(URI sourceFile, Path localTargetFile) throws IOException {
        get(sourceFile).copyToLocal(sourceFile, localTargetFile);
    }

    @Override
    public boolean delete(URI uri) throws IOException {
        return get(uri).delete(uri);
    }

    @Override
    public long size(URI uri) throws IOException {
        return get(uri).size(uri);
    }

    @Override
    public String md5(URI uri) throws IOException {
        return get(uri).md5(uri);
    }
}

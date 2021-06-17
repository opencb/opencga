package org.opencb.opencga.storage.core.io.managers;

import org.apache.solr.common.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.storage.IOConfiguration;
import org.opencb.opencga.core.config.storage.IOConfiguration.IOConnectorConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;

/**
 * Created on 01/05/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class IOConnectorProvider implements IOConnector {

    private final List<IOConnector> ioConnectors;

    public IOConnectorProvider(StorageConfiguration configuration) {
        IOConfiguration io = configuration.getIo();
        if (io == null) {
            io = new IOConfiguration();
        }
        if (io.getConnectors() == null) {
            io.setConnectors(new LinkedHashMap<>());
        }
        ioConnectors = new ArrayList<>(io.getConnectors().size());

        if (!io.getConnectors().containsKey("local")) {
            ioConnectors.add(new LocalIOConnector());
        }

        for (Map.Entry<String, IOConnectorConfiguration> entry : io.getConnectors().entrySet()) {
            Class<? extends IOConnector> aClass = parseClass(entry.getKey(), entry.getValue().getClazz());
            ioConnectors.add(newInstance(aClass, entry.getValue()));
        }

    }

    public IOConnectorProvider(Class<? extends IOConnector>... classes) {
        this(new ObjectMap(), classes);
    }

    public IOConnectorProvider(ObjectMap options, Class<? extends IOConnector>... classes) {
        ioConnectors = new ArrayList<>(classes.length);
        for (Class<? extends IOConnector> aClass : classes) {
            ioConnectors.add(newInstance(aClass, options));
        }
    }

    @SuppressWarnings("unchecked")
    protected Class<? extends IOConnector> parseClass(String ioConnectorName, String ioConnectorClazz) {
        Class<?> aClass;
        if (StringUtils.isEmpty(ioConnectorClazz)) {
            switch (ioConnectorName) {
                case "local":
                    return LocalIOConnector.class;
                case "azure":
                    return AzureBlobStorageIOConnector.class;
                default:
                    throw new IllegalArgumentException("Unknown connector " + ioConnectorName);
            }
        }
        try {
            aClass = Class.forName(ioConnectorClazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
        if (IOConnector.class.isAssignableFrom(aClass)) {
            return (Class<? extends IOConnector>) aClass;
        } else {
            throw new IllegalArgumentException("Class " + aClass + " is not an instance of " + IOConnector.class);
        }
    }

    protected IOConnector newInstance(Class<? extends IOConnector> aClass, ObjectMap options) {
        try {
            if (options != null) {
                for (Constructor<?> constructor : aClass.getConstructors()) {
                    if (constructor.getParameterCount() == 1
                            && constructor.getParameters()[0].getType().isAssignableFrom(ObjectMap.class)) {
                        return (IOConnector) constructor.newInstance(options);
                    }
                }
            }
            return aClass.newInstance();
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public synchronized void add(IOConnector ioConnector) {
        if (!ioConnectors.contains(ioConnector)) {
            ioConnectors.add(ioConnector);
        }
    }

    public IOConnector get(URI uri) throws IOException {
        if (uri == null) {
            return null;
        }
        for (IOConnector ioConnector : ioConnectors) {
            if (ioConnector.isValid(uri)) {
                return ioConnector;
            }
        }
        throw new IOException("Unsupported file: " + uri);
    }

    @Override
    public boolean isValid(URI uri) {
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

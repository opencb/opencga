package org.opencb.opencga.storage.hadoop.variant.search.pending.index.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsFileBasedManager;

import java.io.IOException;
import java.net.URI;

public class SecondaryIndexPendingVariantsFileBasedManager extends PendingVariantsFileBasedManager {

    private static final String SUBFOLDER = "pending_secondary_annotation_index";
    private final SecondaryIndexPendingVariantsFileBasedDescriptor descriptor;

    public SecondaryIndexPendingVariantsFileBasedManager(URI pendingVariantsDir, Configuration conf)
            throws IOException {
        super(pendingVariantsDir, conf);
        descriptor = new SecondaryIndexPendingVariantsFileBasedDescriptor();
    }

    public SecondaryIndexPendingVariantsFileBasedManager(String variantTableName, Configuration conf) throws IOException {
        super(getUri(variantTableName, conf), conf);
        descriptor = new SecondaryIndexPendingVariantsFileBasedDescriptor();
    }

    @Override
    protected SecondaryIndexPendingVariantsFileBasedDescriptor getDescriptor() {
        return descriptor;
    }

    @Override
    public URI getHomeUri() throws IOException {
        return getHomeUri(conf);
    }

    @Override
    public String getSubfolder() {
        return SUBFOLDER;
    }

    private static URI getHomeUri(Configuration conf) throws IOException {
        Path rootPath = HDFSIOConnector.getHdfsRootPath(conf);
        FileSystem fs;
        if (rootPath == null) {
            fs = FileSystem.get(conf);
        } else {
            fs = rootPath.getFileSystem(conf);
        }

        return fs.getUri().resolve(FileSystem.USER_HOME_PREFIX + "/opencga/");
    }

    private static URI getUri(String variantTableName, Configuration conf) throws IOException {
        return getHomeUri(conf).resolve(variantTableName + "/" + SUBFOLDER + "/");
    }

    @Override
    public ObjectMap discoverPending(MRExecutor mrExecutor, String variantsTable, ObjectMap options) throws StorageEngineException {
        options = new ObjectMap(options);
        // Never filter by study!
        options.remove(VariantQueryParam.STUDY.key());
        return super.discoverPending(mrExecutor, variantsTable, options);
    }
}

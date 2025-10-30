package org.opencb.opencga.storage.hadoop.variant.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.opencb.opencga.storage.core.metadata.local.LocalVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.io.HDFSIOConnector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsLocalVariantStorageMetadataDBAdaptorFactory extends LocalVariantStorageMetadataDBAdaptorFactory {

    public HdfsLocalVariantStorageMetadataDBAdaptorFactory(Configuration conf) throws IOException {
        super(getCacheFiles(conf), new HDFSIOConnector(conf));
    }

    private static URI[] getCacheFiles(Configuration conf) throws IOException {
        URI[] cacheFiles;
        Path[] cacheFilesPaths = DistributedCache.getLocalCacheFiles(conf);
        if (cacheFilesPaths != null) {
            cacheFiles = Arrays.stream(cacheFilesPaths)
                    .map(path -> {
                        URI uri = path.toUri();
                        if (uri.getScheme() == null) {
                            // Ensure schema is set. As this is a local file, assume file://
                            try {
                                uri = new URI("file", uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                                        uri.getQuery(), uri.getFragment());
                            } catch (URISyntaxException e) {
                                // This should never happen
                                throw new RuntimeException(e);
                            }
                        }
                        return uri;
                    }).toArray(URI[]::new);
        } else {
            cacheFiles = DistributedCache.getCacheFiles(conf);
        }
        return cacheFiles;
    }

}

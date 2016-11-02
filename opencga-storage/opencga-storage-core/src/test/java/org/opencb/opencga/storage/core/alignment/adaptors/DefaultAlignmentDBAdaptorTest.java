package org.opencb.opencga.storage.core.alignment.adaptors;

import org.junit.Test;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.opencga.storage.core.alignment.iterators.AlignmentIterator;
import org.opencb.opencga.storage.core.alignment.local.DefaultAlignmentStorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Created by pfurio on 26/10/16.
 */
public class DefaultAlignmentDBAdaptorTest {

    @Test
    public void iterator() throws Exception {
        String inputPath = getClass().getResource("/HG00096.chrom20.small.bam").getPath();
        DefaultAlignmentStorageManager defaultAlignmentStorageManager =
                new DefaultAlignmentStorageManager(null, null, new StorageConfiguration(), Paths.get("/tmp"));
        AlignmentIterator iterator = defaultAlignmentStorageManager.getDBAdaptor().iterator(inputPath);
        while (iterator.hasNext()) {
            System.out.println(iterator.next().toString());
        }
    }

}
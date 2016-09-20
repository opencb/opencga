package org.opencb.opencga.analysis.storage.variant;

import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.storage.AbstractAnalysisFileIndexerTest;
import org.opencb.opencga.analysis.variant.VariantFileIndexer;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.FileIndex;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.*;
import static org.opencb.opencga.analysis.variant.VariantFileIndexer.*;

/**
 * Created on 19/09/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantFileIndexerTest extends AbstractAnalysisFileIndexerTest {

    @Test
    public void testIndex() throws Exception {

        VariantFileIndexer variantFileIndexer = new VariantFileIndexer(opencga.getCatalogManager().getCatalogConfiguration(),
                opencga.getStorageConfiguration());

        File file = create("variant-test-file.vcf.gz");
        Path outdir1 = opencga.getOpencgaHome().resolve("job1");
        Files.createDirectory(outdir1);
        Path outdir2 = opencga.getOpencgaHome().resolve("job2");
        Files.createDirectory(outdir2);

        variantFileIndexer.index(file.getName(), outdir1.toString(), sessionId, new QueryOptions(TRANSFORM, true).append(CATALOG_PATH, "data/"));
        file = opencga.getCatalogManager().getFile(file.getId(), sessionId).first();
        assertEquals(FileIndex.IndexStatus.TRANSFORMED, file.getIndex().getStatus().getName());

        variantFileIndexer.index(file.getName(), outdir2.toString(), sessionId, new QueryOptions(LOAD, true).append(CATALOG_PATH, "data/"));
        file = opencga.getCatalogManager().getFile(file.getId(), sessionId).first();
        assertEquals(FileIndex.IndexStatus.READY, file.getIndex().getStatus().getName());
    }

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }
}
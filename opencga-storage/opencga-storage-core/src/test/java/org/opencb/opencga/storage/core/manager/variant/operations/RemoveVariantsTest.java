package org.opencb.opencga.storage.core.manager.variant.operations;

import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.storage.core.manager.variant.AbstractVariantStorageOperationTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 10/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class RemoveVariantsTest extends AbstractVariantStorageOperationTest {

    @Override
    protected VariantSource.Aggregation getAggregation() {
        return VariantSource.Aggregation.NONE;
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testLoadAndRemoveOne() throws Exception {

        File file77 = create("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        File file78 = create("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        indexFile(file77, new QueryOptions(), outputId);
        indexFile(file78, new QueryOptions(), outputId);

        removeFile(file77, new QueryOptions(), outputId);
    }

    private void removeFile(File file, QueryOptions options, long outputId) throws Exception {
        removeFile(Collections.singletonList(file), options, outputId);
    }
    private void removeFile(List<File> files, QueryOptions options, long outputId) throws Exception {
        List<String> fileIds = files.stream().map(File::getId).map(String::valueOf).collect(Collectors.toList());

        long studyId = catalogManager.getStudyIdByFileId(files.get(0).getId());

        variantManager.removeFile(fileIds, String.valueOf(studyId), sessionId);
    }

    @Test
    public void testLoadAndRemoveMany() throws Exception {
        List<File> files = new ArrayList<>();
        for (int i = 77; i <= 93; i++) {
            files.add(create("platinum/1K.end.platinum-genomes-vcf-NA128" + i + "_S1.genome.vcf.gz"));
        }
        indexFiles(files, new QueryOptions(), outputId);



    }

}

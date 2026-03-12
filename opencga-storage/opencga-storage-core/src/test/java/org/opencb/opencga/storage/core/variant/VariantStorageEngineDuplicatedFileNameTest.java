package org.opencb.opencga.storage.core.variant;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StorageEngineTest;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;

import java.net.URI;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Ignore
@StorageEngineTest
public abstract class VariantStorageEngineDuplicatedFileNameTest extends VariantStorageBaseTest {

    @Test
    public void test() throws Exception {
        String fileName = "file.vcf.gz";
        URI file1 = getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.vcf.gz", "folder1/" + fileName);

        VariantStorageEngine engine = getVariantStorageEngine();
        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME);
        runETL(engine, file1, Paths.get(file1).getParent().toUri(), params);

        // Variants should be returned with the fileName as fileId
        for (Variant variant : engine.iterable(new VariantQuery().includeFileAll(), new QueryOptions())) {
            Assert.assertEquals(fileName, variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
        }
        for (Variant variant : engine.iterable(new VariantQuery().file(fileName), new QueryOptions())) {
            Assert.assertEquals(fileName, variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
        }
        // Query by filePath should not fail
        for (Variant variant : engine.iterable(new VariantQuery().file(file1.getPath()), new QueryOptions())) {
            Assert.assertEquals("file.vcf.gz", variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
        }

        URI file2 = getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.vcf.gz", "folder2/" + fileName);
        runETL(engine, file2, Paths.get(file2).getParent().toUri(), params);

        checkDuplicatedFileStatus(engine, file1, file2, fileName);
    }

    private static void checkDuplicatedFileStatus(VariantStorageEngine engine, URI file1, URI file2, String fileName) {
        // Variants should now be returned with the full fileUri as fileId
        for (Variant variant : engine.iterable(new VariantQuery().includeFileAll(), new QueryOptions())) {
            for (FileEntry file : variant.getStudy(STUDY_NAME).getFiles()) {
                Assert.assertThat(file.getFileId(),
                        org.hamcrest.CoreMatchers.anyOf(
                                org.hamcrest.CoreMatchers.is(file1.getPath()),
                                org.hamcrest.CoreMatchers.is(file2.getPath())
                        ));
            }
        }
        for (Variant variant : engine.iterable(new VariantQuery().file(file1.getPath()), new QueryOptions())) {
            Assert.assertEquals(file1.getPath(), variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
        }
        for (Variant variant : engine.iterable(new VariantQuery().file(file2.getPath()), new QueryOptions())) {
            Assert.assertEquals(file2.getPath(), variant.getStudy(STUDY_NAME).getFiles().get(0).getFileId());
        }
        // Query by fileId should fail
        try {
            engine.get(new VariantQuery().file(fileName), new QueryOptions());
            Assert.fail("Should not be able to query by file Id");
        } catch (VariantQueryException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testConcurrentLoading() throws Exception {
        VariantStorageEngine engine = getVariantStorageEngine();

        ObjectMap params = new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME);

        String fileName = "file.vcf.gz";
        URI file1 = getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12877_S1.vcf.gz", "folder1/" + fileName);
        URI file2 = getResourceUri("platinum/1K.end.platinum-genomes-vcf-NA12878_S1.vcf.gz", "folder2/" + fileName);

        ExecutorService service = Executors.newFixedThreadPool(2);
        try {
            Future<?> future1 = service.submit(() -> {
                try {
                    runETL(engine, file1, Paths.get(file1).getParent().toUri(), params);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Future<?> future2 = service.submit(() -> {
                try {
                    runETL(engine, file2, Paths.get(file2).getParent().toUri(), params);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            future1.get();
            future2.get();
        } finally {
            service.shutdown();
        }

        checkDuplicatedFileStatus(engine, file1, file2, fileName);
    }

}

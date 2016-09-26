package org.opencb.opencga.analysis.storage.variant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.storage.AnalysisFileIndexer;
import org.opencb.opencga.analysis.storage.OpenCGATestExternalResource;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.monitor.executors.old.ExecutorManager;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

/**
 * Created on 18/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AnnotationVariantStorageTest {

    @Rule
    public OpenCGATestExternalResource opencga = new OpenCGATestExternalResource();

    protected CatalogManager catalogManager;
    protected String sessionId;
    protected long studyId;
    protected long outputId;
    protected Logger logger = LoggerFactory.getLogger(StatsVariantStorageTest.class);

    private final String userId = "user";
    private final String dbName = "opencga_variants_test";

    @Before
    public void setUp() throws Exception {
        catalogManager = opencga.getCatalogManager();
        opencga.clearStorageDB(dbName);

        catalogManager.createUser(userId, "User", "user@email.org", "user", "ACME", null, null).first();
        sessionId = catalogManager.login(userId, "user", "localhost").first().getString("sessionId");
        long projectId = catalogManager.createProject("p1", "p1", "Project 1", "ACME", null, sessionId).first().getId();
        studyId = catalogManager.createStudy(projectId, "s1", "s1", Study.Type.CASE_CONTROL, null, null, null, null, null, null,
                Collections.singletonMap(File.Bioformat.VARIANT, new DataStore(opencga.getStorageConfiguration().getDefaultStorageEngineId(), dbName)), null, null, null, sessionId).first().getId();
        outputId = catalogManager.createFolder(studyId, Paths.get("data", "index"), false, null, sessionId).first().getId();

        File file = opencga.createFile(studyId, "variant-test-file.vcf.gz", sessionId);

        AnalysisFileIndexer analysisFileIndexer = new AnalysisFileIndexer(catalogManager);
        Job job = analysisFileIndexer.index(file.getId(), outputId, sessionId, new QueryOptions(ExecutorManager.EXECUTE, true)).first();
        System.out.println("job = " + job);

    }

    @Test
    public void testAnnotateDefault() throws Exception {

        VariantStorage variantStorage = new VariantStorage(catalogManager);

        variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> true);
    }

    @Test
    public void testAnnotateRegion() throws Exception {

        VariantStorage variantStorage = new VariantStorage(catalogManager);

        variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "22")
                .append(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> v.getChromosome().equals("22"));

        variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), "1")
                .append(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> v.getChromosome().equals("22") || v.getChromosome().equals("1"));
    }

    @Test
    public void testAnnotateCreateAndLoad() throws Exception {

        VariantStorage variantStorage = new VariantStorage(catalogManager);

        Job job = variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions(AnalysisFileIndexer.CREATE, true)
                .append(ExecutorManager.EXECUTE, true)).first();

        System.out.println("job = " + job);
        File annotFile = catalogManager.searchFile(studyId,
                new Query(FileDBAdaptor.QueryParams.ID.key(), job.getOutput())
                        .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON), null, sessionId).first();

        checkAnnotation(v -> false);

        job = variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions(AnalysisFileIndexer.LOAD, annotFile.getId())
                .append(ExecutorManager.EXECUTE, true)).first();
        System.out.println("job = " + job);

        checkAnnotation(v -> true);
    }

    @Test
    public void testCustomAnnotation() throws Exception {

        VariantStorage variantStorage = new VariantStorage(catalogManager);

        variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions(ExecutorManager.EXECUTE, true));

        checkAnnotation(v -> true);

        File file = opencga.createFile(studyId, "custom_annotation/myannot.gff", sessionId);
        Job job = variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions()
                .append(AnalysisFileIndexer.LOAD, file.getId())
                .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, "myAnnot")
                .append(ExecutorManager.EXECUTE, true)).first();
        System.out.println("job = " + job);

        Assert.assertEquals(Collections.singleton("myAnnot"), checkAnnotation(v -> true));

        file = opencga.createFile(studyId, "custom_annotation/myannot.bed", sessionId);
        variantStorage.annotateVariants(studyId, outputId, sessionId, new QueryOptions()
                .append(AnalysisFileIndexer.LOAD, file.getId())
                .append(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, "myAnnot2")
                .append(ExecutorManager.EXECUTE, true));

        Assert.assertEquals(new HashSet<>(Arrays.asList("myAnnot", "myAnnot2")), checkAnnotation(v -> true));
    }

    public Set<String> checkAnnotation(Function<Variant, Boolean> contains) throws CatalogException, StorageManagerException {
        VariantFetcher variantFetcher = new VariantFetcher(catalogManager, opencga.getStorageManagerFactory());
        VariantDBIterator iterator = variantFetcher.iterator(new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyId),
                new QueryOptions(QueryOptions.SORT, true), sessionId);

        Set<String> customAnnotationKeySet = new LinkedHashSet<>();
        int c = 0;
        while (iterator.hasNext()) {
            c++;
            Variant next = iterator.next();
            if (contains.apply(next)) {
                Assert.assertNotNull(next.getAnnotation());
                if (next.getAnnotation().getAdditionalAttributes() != null) {
                    customAnnotationKeySet.addAll(next.getAnnotation().getAdditionalAttributes().keySet());
                }
            } else {
                Assert.assertNull(next.getAnnotation());
            }
        }
        Assert.assertTrue(c > 0);
        return customAnnotationKeySet;
    }

}

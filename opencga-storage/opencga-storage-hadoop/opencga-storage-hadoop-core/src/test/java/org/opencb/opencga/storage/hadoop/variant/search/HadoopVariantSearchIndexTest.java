package org.opencb.opencga.storage.hadoop.variant.search;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.search.VariantSearchIndexTest;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchLoadResult;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.search.pending.index.file.SecondaryIndexPendingVariantsFileBasedManager;

import java.net.URI;
import java.util.*;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(LongTests.class)
public class HadoopVariantSearchIndexTest extends VariantSearchIndexTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopSolrSupport solrSupport = new HadoopSolrSupport();

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    private int i = 0;

    @Override
    public VariantSearchLoadResult searchIndex(Query query, boolean overwrite) throws Exception {
        i++;
        VariantHadoopDBAdaptor dbAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor();
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri("searchIndex_" + TimeUtils.getTime() + "_" + i + "_pre"));

        externalResource.flush(dbAdaptor.getVariantTable());
        VariantSearchLoadResult loadResult = super.searchIndex(query, overwrite);
        externalResource.flush(dbAdaptor.getVariantTable());
        System.out.println("[" + i + "] VariantSearch LoadResult " + loadResult);
        VariantHbaseTestUtils.printVariants(dbAdaptor, newOutputUri("searchIndex_" + TimeUtils.getTime() + "_" + i + "_post"));
        return loadResult;
    }

    @Test
    public void testRunDiscoverPendingTwice() throws Exception {

        URI file = getPlatinumFile(1);
        runETL(variantStorageEngine, file, "study", new ObjectMap());
        VariantHadoopDBAdaptor dbAdaptor = ((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor();

        SearchIndexMetadata indexMetadata = variantStorageEngine.getVariantSearchManager().newIndexMetadata();
        SecondaryIndexPendingVariantsFileBasedManager pendingVariantsFileBasedManager = new SecondaryIndexPendingVariantsFileBasedManager(dbAdaptor.getVariantTable(), dbAdaptor.getConfiguration());

        pendingVariantsFileBasedManager.discoverPending(((HadoopVariantStorageEngine) variantStorageEngine).getMRExecutor(),
                dbAdaptor.getVariantTable(), false, new ObjectMap(VariantQueryParam.REGION.key(), "1"));

        pendingVariantsFileBasedManager.discoverPending(((HadoopVariantStorageEngine) variantStorageEngine).getMRExecutor(),
                dbAdaptor.getVariantTable(), false, new ObjectMap(VariantQueryParam.REGION.key(), "2"));

        pendingVariantsFileBasedManager.discoverPending(((HadoopVariantStorageEngine) variantStorageEngine).getMRExecutor(),
                dbAdaptor.getVariantTable(), false, new ObjectMap(VariantQueryParam.REGION.key(), "3"));

        pendingVariantsFileBasedManager.discoverPending(((HadoopVariantStorageEngine) variantStorageEngine).getMRExecutor(),
                dbAdaptor.getVariantTable(), false, new ObjectMap());

    }

    @Test
    public void testSearchIndexWhileStatsIndex() throws Exception {
        // Test what would happen if a variantStats or variantAnnotation were being executed while running the variant-secondary-annotation-index

        URI file = getPlatinumFile(1);
        runETL(variantStorageEngine, file, "study", new ObjectMap());
        HadoopVariantStorageEngine variantStorageEngine = (HadoopVariantStorageEngine) this.variantStorageEngine;
        VariantHadoopDBAdaptor dbAdaptor = variantStorageEngine.getDBAdaptor();
        Long variants = dbAdaptor.count().first();

        // Create empty search index
        SearchIndexMetadata indexMetadata = variantStorageEngine.getVariantSearchManager().newIndexMetadata();
        variantStorageEngine.getVariantSearchManager().createCollections(indexMetadata);

        // Run DiscoverPendingVariants and update timestamp
        Assert.assertTrue(variantStorageEngine.shouldRunDiscoverPendingVariantsSecondaryAnnotationIndex(variantStorageEngine.getVariantSearchManager().getSearchIndexMetadata(), false));
        variantStorageEngine.runDiscoverPendingVariantsSecondaryAnnotationIndex(new Query(), new QueryOptions(), false, indexMetadata, System.currentTimeMillis());
        Assert.assertFalse(variantStorageEngine.shouldRunDiscoverPendingVariantsSecondaryAnnotationIndex(variantStorageEngine.getVariantSearchManager().getSearchIndexMetadata(), false));

        // Update variant stats. This stats won't be included in the DiscoverPendingVariants files.
        variantStorageEngine.calculateStats("study", Collections.singletonList(StudyEntry.DEFAULT_COHORT), new QueryOptions());
        Assert.assertTrue(variantStorageEngine.shouldRunDiscoverPendingVariantsSecondaryAnnotationIndex(variantStorageEngine.getVariantSearchManager().getSearchIndexMetadata(), false));

        // Run secondaryAnnotationIndex but skip DiscoverPendingVariants. Should load search index without stats.
        variantStorageEngine.getOptions().put("skipDiscoverPendingVariantsToSecondaryIndex", true);
        VariantSearchLoadResult result = searchIndex();
        variantStorageEngine.getOptions().put("skipDiscoverPendingVariantsToSecondaryIndex", false);
        System.out.println("result = " + result);
        Assert.assertEquals(variants.longValue(), result.getNumInsertedVariants());
        Assert.assertFalse(result.getAttributes().getBoolean("runDiscoverPendingVariantsToSecondaryIndexMr"));

        // It should still need to run the DiscoverPendingVariants
        Assert.assertTrue(variantStorageEngine.shouldRunDiscoverPendingVariantsSecondaryAnnotationIndex(variantStorageEngine.getVariantSearchManager().getSearchIndexMetadata(), false));
        result = searchIndex();
        System.out.println("result = " + result);
        Assert.assertEquals(variants.longValue(), result.getNumLoadedVariantsPartialStatsUpdate());
        Assert.assertTrue(result.getAttributes().getBoolean("runDiscoverPendingVariantsToSecondaryIndexMr"));
    }

    @Test
    public void testUpdatePartialStatsSpecificCohorts() throws Exception {

        URI file = getPlatinumFile(1);
        runETL(variantStorageEngine, file, "study", new ObjectMap());
        HadoopVariantStorageEngine variantStorageEngine = (HadoopVariantStorageEngine) this.variantStorageEngine;

        List<String> samples = new ArrayList<>(1);
        for (SampleMetadata sampleMetadata : metadataManager.sampleMetadataIterable(metadataManager.getStudyId("study"))) {
            samples.add(sampleMetadata.getName());
        }
        Map<String, Collection<String>> cohorts = new HashMap<>();
        cohorts.put("cohort1", samples);
        variantStorageEngine.calculateStats("study", cohorts, new QueryOptions());


        searchIndex();

        cohorts = new HashMap<>();
        cohorts.put("cohort2", samples);
        variantStorageEngine.calculateStats("study", cohorts, new QueryOptions());

        variantStorageEngine.runDiscoverPendingVariantsSecondaryAnnotationIndex(new Query(), new QueryOptions(), false, variantStorageEngine.getVariantSearchManager().getSearchIndexMetadata(), System.currentTimeMillis());
        SecondaryIndexPendingVariantsFileBasedManager pendingManager = new SecondaryIndexPendingVariantsFileBasedManager(variantStorageEngine.getVariantTableName(), variantStorageEngine.getConf());

        int count = 0;
        for (Variant variant : pendingManager.reader(new Query())) {
            count++;
            Assert.assertEquals(1, variant.getStudies().get(0).getStats().size());
            Assert.assertEquals("cohort2", variant.getStudies().get(0).getStats().get(0).getCohortId());
        }
        Assert.assertNotEquals(0, count);
        Assert.assertNotEquals(0, pendingManager.reader(new VariantQuery().region("1")).stream().count());

        searchIndex(new VariantQuery().region("1"), false);

        Assert.assertEquals(0, pendingManager.reader(new VariantQuery().region("1")).stream().count());

        for (Variant variant : pendingManager.reader(new Query())) {
            Assert.assertEquals(1, variant.getStudies().get(0).getStats().size());
            Assert.assertEquals(1, variant.getStudies().get(0).getStats().size());
            Assert.assertNotEquals("1", variant.getChromosome());

            Assert.assertEquals("cohort2", variant.getStudies().get(0).getStats().get(0).getCohortId());
        }
    }

    @Test
    public void testSearchIndexWithWeirdContigs() throws Exception {
        URI file = getResourceUri("variant-test-unusual-contigs.vcf");
        runETL(variantStorageEngine, file, "study", new ObjectMap());
        HadoopVariantStorageEngine variantStorageEngine = (HadoopVariantStorageEngine) this.variantStorageEngine;

        searchIndex();
    }

}

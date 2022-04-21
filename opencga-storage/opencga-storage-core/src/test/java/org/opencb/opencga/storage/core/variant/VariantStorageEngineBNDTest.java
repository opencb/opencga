package org.opencb.opencga.storage.core.variant;

import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.exceptions.NonStandardCompliantSampleField;
import org.opencb.biodata.tools.variant.VariantNormalizer;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;

import java.net.URI;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Created on 14/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageEngineBNDTest extends VariantStorageBaseTest {

    protected static StudyMetadata studyMetadata;
    protected static boolean loaded = false;
    protected static StoragePipelineResult pipelineResult1;
    protected static URI input1;

    @Before
    public void before() throws Exception {
        if (!loaded) {
            clearDB(DB_NAME);
            loadFiles();
            loaded = true;
        }
    }

    protected void loadFiles() throws Exception {
        studyMetadata = new StudyMetadata(1, "s1");
//        variantStorageEngine.getOptions().append(VariantStorageOptions.ANNOTATOR_CELLBASE_EXCLUDE.key(), "expression,clinical");
        input1 = getResourceUri("variant-test-bnd.vcf");
        pipelineResult1 = runDefaultETL(input1, variantStorageEngine, studyMetadata, new QueryOptions()
                .append(VariantStorageOptions.ANNOTATE.key(), true)
                .append(VariantStorageOptions.SOMATIC.key(), true));
    }

    @Test
    public void checkAllAnnotated() throws Exception {
        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertNotNull(variant.toString(), variant.getAnnotation());
        }
    }

    @Test
    public void getPairs() throws Exception {
        getPairs(new Query());
        getPairs(new Query(VariantQueryParam.REGION.key(), "2"));
        getPairs(new Query(VariantQueryParam.REGION.key(), "17"));
        getPairs(new Query(VariantQueryParam.REGION.key(), "2,13,5"));
        getPairs(new Query(VariantQueryParam.REGION.key(), "2").append(VariantQueryParam.GENE.key(), "VPS53"));
        getPairs(new Query(VariantQueryParam.GENE.key(), "VPS53"));
        getPairs(new Query(VariantQueryParam.GENE.key(), "BRCA2"));
    }

    public void getPairs(Query inputQuery) throws Exception {
        Set<String> variants = new HashSet<>();
        List<String> variantsList = new LinkedList<>();
        Set<String> duplicatedVariants = new HashSet<>();
        String prevId = null;
        String prevMateid = null;
        System.out.println("-----" + inputQuery.toJson());
        Query query = new Query(inputQuery)
                .append(VariantQueryParam.TYPE.key(), VariantType.BREAKEND.toString())
                .append(VariantQueryParam.SAMPLE.key(), "SAMPLE_BND1");
        for (Variant variant : variantStorageEngine.iterable(query, new QueryOptions())) {
            Map<String, String> info = variant.getStudies().get(0).getFile(0).getData();
            String id = info.get(StudyEntry.VCF_ID);
            String mateid = info.get("MATEID");
            if (prevId != null) {
                assertEquals(id, prevMateid);
                assertEquals(mateid, prevId);
                prevId = null;
                prevMateid = null;
            } else {
                prevId = id;
                prevMateid = mateid;
            }
            System.out.println("variant = " + variant + " " + id + " " + mateid);
            variantsList.add(variant.toString());
            if (!variants.add(variant.toString())) {
                duplicatedVariants.add(variant.toString());
            }
        }
        assertNull(prevMateid);
        assertNull(prevId);
        assertThat(duplicatedVariants, CoreMatchers.not(CoreMatchers.hasItem(CoreMatchers.anything())));

        // Check pagination
        testPagination(variantsList, query, 1);
        testPagination(variantsList, query, 2);
        testPagination(variantsList, query, 3);
        testPagination(variantsList, query, 4);
        testPagination(variantsList, query, 5);
        testPagination(variantsList, query, 200);
    }

    private void testPagination(List<String> variantsList, Query query, int batchSize) {
        List<String> actualVariants = new ArrayList<>(variantsList.size());
        for (int i = 0; i < variantsList.size(); i += batchSize) {
            QueryOptions options = new QueryOptions(QueryOptions.LIMIT, batchSize)
                    .append(QueryOptions.SKIP, i);
            List<Variant> results = variantStorageEngine.get(query, options).getResults();
            System.out.println("options = " + options.toJson() + " -> " + results.size());
            for (Variant result : results) {
                actualVariants.add(result.toString());
            }
            assertTrue(results.size() <= batchSize);
        }
        assertEquals(variantsList, actualVariants);
    }

    @Test
    public void checkCorrectnessFile1() throws Exception {
        checkCorrectness(VariantStorageEngineBNDTest.input1);
    }

    protected void checkCorrectness(URI file) throws StorageEngineException, NonStandardCompliantSampleField {
        Map<String, Variant> expectedVariants = readVariants(file);

        VariantDBIterator variants = variantStorageEngine.getDBAdaptor()
                .iterator(new Query(VariantQueryParam.STUDY.key(), "s1")
                                .append(VariantQueryParam.FILE.key(), Paths.get(file).getFileName().toString())
                                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true),
                        new QueryOptions(QueryOptions.SORT, true));
        while (variants.hasNext()) {
            Variant actual = variants.next();
            Variant expected = expectedVariants.get(actual.toString());

            // Check variants are correct
            assertNotNull(actual.toString(), expected);
            assertEquals(expected.toString(), actual.toString());
            assertEquals(expected.getSv(), actual.getSv());

            StudyEntry actualStudyEntry = actual.getStudies().get(0);
            StudyEntry expectedStudyEntry = expected.getStudies().get(0);

            // Check GTs are correct
            assertEquals(
                    expectedStudyEntry.getSamples()
                            .stream()
                            .map(l -> new SampleEntry(null, 0, Collections.singletonList(l.getData().get(0))))
                            .collect(Collectors.toList()),
                    actualStudyEntry.getSamples());

            // Check File Attributes are correct
            expectedStudyEntry.getFiles().get(0).setFileId("");
            actualStudyEntry.getFiles().get(0).setFileId("");
            assertEquals(expectedStudyEntry.getFiles().get(0), actualStudyEntry.getFiles().get(0));


            if (actual.getAlternate().equals("<DEL:ME:ALU>") || actual.getType().equals(VariantType.BREAKEND)) {
                System.err.println("WARN: Variant " + actual + (actual.getAnnotation() == null ? " without annotation" : " with annotation"));
            } else {
                assertNotNull(actual.toString(), actual.getAnnotation());
            }
        }
    }

    @Test
    public void exportVcf() throws Exception {
        variantStorageEngine.exportData(null, VariantWriterFactory.VariantOutputFormat.VCF, null, new Query(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./."), new QueryOptions(QueryOptions.SORT, true));
    }

    protected Map<String, Variant> readVariants(URI input) throws StorageEngineException, NonStandardCompliantSampleField {
        VariantReader variantReader = variantReaderUtils.getVariantReader(Paths.get(input));
        variantReader.open();
        variantReader.pre();
        List<Variant> variants = variantReader.read(1000);
        VariantNormalizer normalizer = new VariantNormalizer(new VariantNormalizer.VariantNormalizerConfig()
                .setReuseVariants(true)
                .setNormalizeAlleles(true));
        variants = normalizer
                .normalize(variants, true);
        variantReader.post();
        variantReader.close();

        return variants.stream().collect(Collectors.toMap(Variant::toString, v -> v));
    }
}

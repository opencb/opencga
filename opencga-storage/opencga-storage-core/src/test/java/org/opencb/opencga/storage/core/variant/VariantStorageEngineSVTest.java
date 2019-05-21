package org.opencb.opencga.storage.core.variant;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created on 14/09/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Ignore
public abstract class VariantStorageEngineSVTest extends VariantStorageBaseTest {

    protected static StudyMetadata studyMetadata;
    protected static boolean loaded = false;
    protected static StoragePipelineResult pipelineResult1;
    protected static StoragePipelineResult pipelineResult2;
    protected static URI input1;
    protected static URI input2;

    @Before
    public void before() throws Exception {
        if (!loaded) {
            clearDB(DB_NAME);
            loadFiles();
            loaded = true;
        }
    }

    protected void loadFiles() throws Exception {
        input1 = getResourceUri("variant-test-sv.vcf");
        studyMetadata = new StudyMetadata(1, "s1");
        pipelineResult1 = runDefaultETL(input1, variantStorageEngine, studyMetadata, new QueryOptions()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true));
        input2 = getResourceUri("variant-test-sv_2.vcf");
        pipelineResult2 = runDefaultETL(input2, variantStorageEngine, studyMetadata, new QueryOptions()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), true));
    }

    @Test
    public void checkAllAnnotated() throws Exception {
        for (Variant variant : variantStorageEngine.getDBAdaptor()) {
            assertNotNull(variant.toString(), variant.getAnnotation());
        }
    }

    @Test
    public void checkCount() throws Exception {
        int expected = 24 + 7;
        int count = variantStorageEngine.getDBAdaptor().count(null).first().intValue();
        assertEquals(expected, count);
    }

    @Test
    public void checkCorrectnessFile1() throws Exception {
        checkCorrectness(VariantStorageEngineSVTest.input1);
    }

    @Test
    public void checkCorrectnessFile2() throws Exception {
        checkCorrectness(VariantStorageEngineSVTest.input2);
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
            assertEquals(expectedStudyEntry.getSamplesData().stream().map(l -> Collections.singletonList(l.get(0))).collect(Collectors.toList()), actualStudyEntry.getSamplesData());

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

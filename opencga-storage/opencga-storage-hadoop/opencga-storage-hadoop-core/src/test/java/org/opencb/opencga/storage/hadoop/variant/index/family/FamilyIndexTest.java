package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.AbstractSampleIndexEntryFilter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema;

import java.net.URI;
import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;

/**
 * Created on 12/03/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FamilyIndexTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {


    private static boolean loaded = false;
    private static String study = "study";
    private static String father = "NA12877";
    private static String mother = "NA12878";
    private static String child = "NA12879";  // Maybe this is not accurate, but works file for the example

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();
    private SampleIndexQueryParser sampleIndexQueryParser;

    @Before
    public void before() throws Exception {
        if (!loaded) {
            HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
            URI outputUri = newOutputUri();

            ObjectMap params = new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                    .append(VariantStorageOptions.STUDY.key(), study);
            runETL(variantStorageEngine, getPlatinumFile(12877), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12878), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12879), outputUri, params, true, true, true);




            List<String> family = Arrays.asList(father, mother, child);

            variantStorageEngine.aggregateFamily(study, family, new ObjectMap());

            variantStorageEngine.familyIndex(study, Collections.singletonList(family), new ObjectMap());


            VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
            loaded = true;
        }
        sampleIndexQueryParser = new SampleIndexQueryParser(metadataManager, SampleIndexSchema.defaultSampleIndexSchema());
    }

    @Test
    public void testMendelianErrors() throws Exception {
        Set<String> mendelianErrorVariants = new HashSet<>();
        Set<String> deNovoVariants = new HashSet<>();
        for (Variant variant : variantStorageEngine) {
            Genotype fatherGenotype = new Genotype(variant.getStudies().get(0).getSampleData(father, "GT"));
            Genotype motherGenotype = new Genotype(variant.getStudies().get(0).getSampleData(mother, "GT"));
            Genotype childGenotype = new Genotype(variant.getStudies().get(0).getSampleData(child, "GT"));
            int meCode = MendelianError.compute(fatherGenotype, motherGenotype, childGenotype, variant.getChromosome());
            if (meCode != 0) {
                mendelianErrorVariants.add(variant.toString());
                if (AbstractSampleIndexEntryFilter.isDeNovo(meCode)) {
                    deNovoVariants.add(variant.toString());
                }
            }
        }

        Query query = new Query()
                .append(VariantQueryParam.SAMPLE.key(), child + ":MendelianError")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResults()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getIssues());
            assertThat(mendelianErrorVariants, hasItem(variant.toString()));
            assertEquals(1, variant.getStudies().get(0).getIssues().size());
            assertEquals(child, variant.getStudies().get(0).getIssues().get(0).getSample().getSampleId());
        }
        assertEquals(mendelianErrorVariants.size(), result.getNumResults());

        query = new Query()
                .append(VariantQueryParam.SAMPLE.key(), child + ":deNovo")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                .append(VariantQueryParam.INCLUDE_FILE.key(), VariantQueryUtils.NONE);
        result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResults()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
            assertThat(deNovoVariants, hasItem(variant.toString()));
        }
        assertEquals(deNovoVariants.size(), result.getNumResults());


        query = new Query()
                .append(VariantQueryParam.SAMPLE.key(), child + ":MendelianError")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                .append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .append(VariantQueryParam.QUAL.key(), ">30");

        result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResults()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
            assertThat(mendelianErrorVariants, hasItem(variant.toString()));
            assertThat(Double.valueOf(variant.getStudies().get(0).getFiles().get(0).getData().get(StudyEntry.QUAL)), gt(30));
        }
        assertNotEquals(0, result.getNumResults());
    }

    @Test
    public void testParentGtCode() {
        VariantQueryResult<Variant> all = variantStorageEngine.get(new Query(VariantQueryParam.INCLUDE_GENOTYPE.key(), true), new QueryOptions());

        Query query = new Query()
                .append(VariantQueryParam.GENOTYPE.key(), child + ":0/1"
                        + ";" + mother + ":0/0")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                /*.append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .append(VariantQueryParam.QUAL.key(), ">30")*/;


        SampleIndexQuery sampleIndexQuery = sampleIndexQueryParser.parse(new Query(query));
        assertEquals(1, sampleIndexQuery.getSamplesMap().size());
        assertNotNull(sampleIndexQuery.getMotherFilterMap().get(child));
        assertNull(sampleIndexQuery.getFatherFilterMap().get(child));

        DataResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());

        for (Variant variant : result.getResults()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
        }
        assertThat(result, everyResult(all, withStudy(study, allOf(
                withSampleData(child, "GT", is("0/1")),
                withSampleData(mother, "GT", is("0/0"))))));
    }

    @Test
    public void testParentsGtCode() {
        VariantQueryResult<Variant> all = variantStorageEngine.get(new Query(VariantQueryParam.INCLUDE_GENOTYPE.key(), true), new QueryOptions());

        Query query = new Query()
                .append(VariantQueryParam.GENOTYPE.key(), child + ":0/1"
                        + ";" + father + ":0/0"
                        + ";" + mother + ":0/0")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                /*.append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .append(VariantQueryParam.QUAL.key(), ">30")*/;

        SampleIndexQuery sampleIndexQuery = sampleIndexQueryParser.parse(new Query(query));
        assertEquals(1, sampleIndexQuery.getSamplesMap().size());
        assertNotNull(sampleIndexQuery.getFatherFilterMap().get(child));
        assertNotNull(sampleIndexQuery.getMotherFilterMap().get(child));


        DataResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResults()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
        }
        assertThat(result, everyResult(all, withStudy(study, allOf(
                withSampleData(child, "GT", is("0/1")),
                withSampleData(father, "GT", is("0/0")),
                withSampleData(mother, "GT", is("0/0"))))));
    }

    @Test
    public void testParentsMissingGtCode() {
        VariantQueryResult<Variant> all = variantStorageEngine.get(new Query(VariantQueryParam.INCLUDE_GENOTYPE.key(), true), new QueryOptions());

        Query query = new Query()
                .append(VariantQueryParam.GENOTYPE.key(), child + ":0/1"
                        + ";" + father + ":./."
                        + ";" + mother + ":./.")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                /*.append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .append(VariantQueryParam.QUAL.key(), ">30")*/;

        assertTrue(SampleIndexQueryParser.validSampleIndexQuery(new Query(query)));
        SampleIndexQuery sampleIndexQuery = sampleIndexQueryParser.parse(new Query(query));
        assertEquals(1, sampleIndexQuery.getSamplesMap().size());
        assertNotNull(sampleIndexQuery.getFatherFilterMap().get(child));
        assertNotNull(sampleIndexQuery.getMotherFilterMap().get(child));


        DataResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResults()) {
            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
        }
        assertThat(result, everyResult(all, withStudy(study, allOf(
                withSampleData(child, "GT", is("0/1")),
                withSampleData(father, "GT", is("./.")),
                withSampleData(mother, "GT", is("./."))))));
    }

    @Test
    public void testParentsMissingAllGtCode() {
        Query query = new Query()
                .append(VariantQueryParam.GENOTYPE.key(), child + ":0/1,./."
                        + ";" + father + ":./."
                        + ";" + mother + ":./.")
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL)
                /*.append(VariantQueryParam.FILE.key(), "1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .append(VariantQueryParam.QUAL.key(), ">30")*/;

        assertFalse(SampleIndexQueryParser.validSampleIndexQuery(new Query(query)));
    }
}

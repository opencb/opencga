package org.opencb.opencga.storage.hadoop.variant.index.family;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Genotype;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.IssueType;
import org.opencb.biodata.tools.pedigree.MendelianError;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.index.query.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.index.sample.AbstractSampleIndexEntryFilter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQueryParser;

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
    private static Set<String> mendelianErrorVariants;
    private static Set<String> deNovoVariants;

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();
    private SampleIndexQueryParser sampleIndexQueryParser;

    @Before
    public void before() throws Exception {
        if (!loaded) {
            HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
            variantStorageEngine.getConfiguration().getCellbase().setUrl("https://ws.zettagenomics.com/cellbase/");
            variantStorageEngine.getConfiguration().getCellbase().setVersion("v5.1");
            variantStorageEngine.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), "grch38");
            variantStorageEngine.reloadCellbaseConfiguration();
            URI outputUri = newOutputUri();

            ObjectMap params = new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                    .append(VariantStorageOptions.STUDY.key(), study);
            runETL(variantStorageEngine, getPlatinumFile(12877), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12878), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12879), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getResourceUri("variant-test-me.vcf"), outputUri, params, true, true, true);


            List<String> family = Arrays.asList(father, mother, child);

            variantStorageEngine.aggregateFamily(study, new VariantAggregateFamilyParams(family, false), new ObjectMap());

            variantStorageEngine.familyIndex(study, Collections.singletonList(family), new ObjectMap());
            variantStorageEngine.familyIndex(study, Collections.singletonList(Arrays.asList("FATHER", "MOTHER", "PROBAND")), new ObjectMap());

            variantStorageEngine.annotate(outputUri, new ObjectMap());

            VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));

            mendelianErrorVariants = new HashSet<>();
            deNovoVariants = new HashSet<>();
            for (Variant variant : variantStorageEngine.iterable(new VariantQuery().includeSampleAll().unknownGenotype("./."), new QueryOptions())) {
                Genotype fatherGenotype = new Genotype(variant.getStudies().get(0).getSampleData(father, "GT"));
                Genotype motherGenotype = new Genotype(variant.getStudies().get(0).getSampleData(mother, "GT"));
                Genotype childGenotype = new Genotype(variant.getStudies().get(0).getSampleData(child, "GT"));
                int meCode = MendelianError.compute(fatherGenotype, motherGenotype, childGenotype, variant.getChromosome());
                if (meCode != 0) {
                    if (AbstractSampleIndexEntryFilter.testDeNovo(meCode, childGenotype.toString(), false)) {
                        mendelianErrorVariants.add(variant.toString());
                        if (AbstractSampleIndexEntryFilter.isDeNovo(meCode)) {
                            deNovoVariants.add(variant.toString());
                        }
                    }
                }
            }

            loaded = true;
        }
        sampleIndexQueryParser = new SampleIndexQueryParser(metadataManager);
    }

    @Test
    public void testMendelianErrors() throws Exception {
        testMendelianErrorVariants(new VariantQuery());
    }
    @Test
    public void testMendelianErrorsBT() throws Exception {
        testMendelianErrorVariants(new VariantQuery().biotype("processed_transcript"));
    }
    @Test
    public void testMendelianErrorsBT_gene() throws Exception {
        testMendelianErrorVariants(new VariantQuery().biotype("processed_transcript").gene("DDX11L1"));
    }
    @Test
    public void testMendelianErrorsQual() throws Exception {
        testMendelianErrorVariants(new VariantQuery()
                .file("1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .qual(">30"));
    }
    @Test
    public void testDeNovo() throws Exception {
        testDeNovoVariants(new VariantQuery());
    }
    @Test
    public void testDeNovoBT() throws Exception {
        testDeNovoVariants(new VariantQuery().biotype("processed_transcript"));
    }
    @Test
    public void testDeNovoQual() throws Exception {
        testDeNovoVariants(new VariantQuery()
                .file("1K.end.platinum-genomes-vcf-" + child + "_S1.genome.vcf.gz")
                .qual(">30"));
    }

    private void testDeNovoVariants(Query baseQuery) {
        Query query = new Query(baseQuery)
                .append(VariantQueryParam.SAMPLE.key(), child + ":denovo");
        Query plainQuery = new Query(baseQuery)
                .append(VariantQueryParam.SAMPLE.key(), child);

        testQuery(deNovoVariants, query, plainQuery);
    }

    private void testMendelianErrorVariants(Query baseQuery) {
        Query query = new Query(baseQuery)
                .append(VariantQueryParam.SAMPLE.key(), child + ":MendelianError");
        Query plainQuery = new Query(baseQuery);

        testQuery(mendelianErrorVariants, query, plainQuery);
    }

    private void testQuery(Set<String> expectedVariants, Query query, Query plainQuery) {
        Set<String> plainQueryVariants = new HashSet<>();
        for (Variant variant : variantStorageEngine.iterable(plainQuery, new QueryOptions())) {
            plainQueryVariants.add(variant.toString());
        }
        VariantQueryResult<Variant> result = variantStorageEngine.get(query, new QueryOptions());
        for (Variant variant : result.getResults()) {
//            System.out.println(variant.toString() + "\t" + variant.getStudies().get(0).getSamples());
            System.out.println(variant.toString() + "\t" + variant.getAnnotation());
            assertThat("expected plain filter", plainQueryVariants, hasItem(variant.toString()));
            assertThat("expected with familyIndex filter", expectedVariants, hasItem(variant.toString()));
        }
        assertNotEquals(0, result.getNumResults());
    }

    @Test
    public void testMultiAllelicMendelianError() {
        Set<String> mendelianErrorVariants = new HashSet<>();
        Set<String> deNovoVariants = new HashSet<>();
        for (Variant variant : variantStorageEngine.iterable(new VariantQuery().file("variant-test-me.vcf"), new QueryOptions())) {
            Genotype probandGt = new Genotype(variant.getStudies().get(0).getSample("PROBAND").getData().get(0));
            Genotype fatherGt = new Genotype(variant.getStudies().get(0).getSample("FATHER").getData().get(0));
            Genotype motherGt = new Genotype(variant.getStudies().get(0).getSample("MOTHER").getData().get(0));
            int meCode = MendelianError.compute(fatherGt, motherGt, probandGt, variant.getChromosome());
            if (meCode != 0) {
                if (AbstractSampleIndexEntryFilter.testDeNovo(meCode, probandGt.toString(), false)) {
                    mendelianErrorVariants.add(variant.toString());
                    if (AbstractSampleIndexEntryFilter.isDeNovo(meCode)) {
                        deNovoVariants.add(variant.toString());
                    }
                }
            }
        }
        assertFalse(mendelianErrorVariants.isEmpty());
        assertFalse(deNovoVariants.isEmpty());

        for (Variant var : variantStorageEngine.get(new VariantQuery().includeSampleId(true).sample("PROBAND:denovo"), new QueryOptions()).getResults()) {
//            System.out.println(VariantQueryUtils.toVcfDebug(var));
            List<StudyEntry> studies = var.getStudies();
            assertTrue(GenotypeClass.MAIN_ALT.test(studies.get(0).getSampleData("PROBAND", "GT")));
            assertEquals(1, studies.get(0).getIssues().size());
            assertEquals(IssueType.DE_NOVO, studies.get(0).getIssues().get(0).getType());
            assertTrue(deNovoVariants.remove(var.toString()));
        }
        // all denovo should have been removed
        assertTrue(deNovoVariants.isEmpty());

        for (Variant var : variantStorageEngine.get(new VariantQuery().includeSampleId(true).sample("PROBAND:mendelianerror"), new QueryOptions()).getResults()) {
//            System.out.println(VariantQueryUtils.toVcfDebug(var));
            List<StudyEntry> studies = var.getStudies();
            assertEquals(1, studies.get(0).getIssues().size());
            if (studies.get(0).getIssues().get(0).getType().equals(IssueType.DE_NOVO)) {
                String gt = studies.get(0).getSampleData("PROBAND", "GT");
                assertTrue(GenotypeClass.MAIN_ALT.test(gt));
            } else {
                assertEquals(IssueType.MENDELIAN_ERROR, studies.get(0).getIssues().get(0).getType());
            }
            assertTrue(mendelianErrorVariants.remove(var.toString()));
        }
        // all mendelian-errors should have been removed
        assertTrue(mendelianErrorVariants.isEmpty());
    }

    @Test
    public void testParentGtCode() {
        VariantQueryResult<Variant> all = variantStorageEngine.get(new Query()
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL), new QueryOptions());

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
        VariantQueryResult<Variant> all = variantStorageEngine.get(new Query()
                        .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), VariantQueryUtils.ALL), new QueryOptions());

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

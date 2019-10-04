package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.*;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.tools.variant.stats.SampleVariantStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.net.URI;
import java.util.*;

public class SampleVariantStatsTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private static boolean loaded = false;
    private static String study = "study";
    private static String father = "NA12877";
    private static String mother = "NA12878";
    private static String child = "NA12879";  // Maybe this is not accurate, but works file for the example
    private static List<SampleVariantStats> stats;

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();
    private int studyId;

    @Before
    public void before() throws Exception {
        if (!loaded) {
            loaded = true;
            HadoopVariantStorageEngine variantStorageEngine = getVariantStorageEngine();
            URI outputUri = newOutputUri();

            ObjectMap params = new ObjectMap(VariantStorageEngine.Options.ANNOTATE.key(), false)
                    .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false)
                    .append(VariantStorageEngine.Options.STUDY.key(), study);
            runETL(variantStorageEngine, getPlatinumFile(12877), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12878), outputUri, params, true, true, true);
            runETL(variantStorageEngine, getPlatinumFile(12879), outputUri, params, true, true, true);




            List<String> family = Arrays.asList(father, mother, child);
            VariantStorageMetadataManager mm = variantStorageEngine.getMetadataManager();
            studyId = mm.getStudyId(study);
            mm.updateSampleMetadata(studyId, mm.getSampleId(studyId, child), sampleMetadata -> {
                sampleMetadata.setFather(mm.getSampleId(studyId, father));
                sampleMetadata.setMother(mm.getSampleId(studyId, mother));
                return sampleMetadata;
            });

            variantStorageEngine.annotate(new Query(), new ObjectMap());
            variantStorageEngine.fillGaps(study, family, new ObjectMap());


            VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));

            stats = computeSampleVariantStatsDirectly();
        }
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Test
    public void test() throws Exception {

        HadoopVariantStorageEngine engine = getVariantStorageEngine();


        ObjectMap params = new ObjectMap(SampleVariantStatsDriver.SAMPLES, father);

        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);

        List<SampleVariantStats> actualStats = new ArrayList<>(3);
        engine.getMetadataManager()
                .sampleMetadataIterator(studyId)
                .forEachRemaining(s -> actualStats.add(s.getStats()));

        Assert.assertEquals(stats.get(0), actualStats.get(0));
        Assert.assertNull(actualStats.get(1));
        Assert.assertNull(actualStats.get(2));

        params.put(SampleVariantStatsDriver.SAMPLES, "auto");
        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);

        actualStats.clear();
        engine.getMetadataManager()
                .sampleMetadataIterator(studyId)
                .forEachRemaining(s -> actualStats.add(s.getStats()));


        Assert.assertEquals(stats, actualStats);
    }


    public List<SampleVariantStats> computeSampleVariantStatsDirectly() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();

        Map<String, String> sampleFileMap = new HashMap<>();
        sampleFileMap.put(father, "1K.end.platinum-genomes-vcf-NA12877_S1.genome.vcf.gz");
        sampleFileMap.put(mother, "1K.end.platinum-genomes-vcf-NA12878_S1.genome.vcf.gz");
        sampleFileMap.put(child, "1K.end.platinum-genomes-vcf-NA12879_S1.genome.vcf.gz");

        Pedigree pedigree = new Pedigree();
        pedigree.setMembers(Collections.singletonList(
                new Member(child, child, Member.Sex.UNKNOWN, Member.AffectionStatus.UNKNOWN)
                        .setFather(new Member(father, father, Member.Sex.MALE, Member.AffectionStatus.UNKNOWN))
                        .setMother(new Member(mother, mother, Member.Sex.FEMALE, Member.AffectionStatus.UNKNOWN))));

        SampleVariantStatsCalculator calculator = new SampleVariantStatsCalculator(pedigree, Arrays.asList(father, mother, child), sampleFileMap);
        List<SampleVariantStats> stats = calculator.compute(engine.iterator());
        stats.forEach(s -> s.setMissingPositions(0)); // Clear this
        return stats;
    }

//    public void print(List<SampleVariantStats> actualStats, List<SampleVariantStats> stats) {
//        System.out.println("-----ACTUAL-----");
//        for (SampleVariantStats actualStat : actualStats) {
//            System.out.println(actualStat.getId());
//            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(actualStat));
//        }
//        System.out.println("-----EXPECTED-----");
//        for (SampleVariantStats stat : stats) {
//            System.out.println(stat.getId());
//            System.out.println(JacksonUtils.getDefaultObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(stat));
//        }
//    }
}
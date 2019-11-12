package org.opencb.opencga.storage.hadoop.variant.stats;

import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.tools.variant.stats.SampleVariantStatsCalculator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class SampleVariantStatsTest extends VariantStorageBaseTest implements HadoopVariantStorageTest {

    private static boolean loaded = false;
    private static String study = "study";
    private static String father = "NA12877";
    private static String mother = "NA12878";
    private static String child = "NA12879";  // Maybe this is not accurate, but works file for the example
    private static List<SampleVariantStats> stats;
    private static int studyId;

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();
    private HadoopVariantStorageEngine engine;

    @Before
    public void before() throws Exception {
        engine = getVariantStorageEngine();
        if (!loaded) {
            loaded = true;
            URI outputUri = newOutputUri();

            ObjectMap params = new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), false)
                    .append(VariantStorageOptions.STUDY.key(), study);
            runETL(engine, getPlatinumFile(12877), outputUri, params, true, true, true);
            runETL(engine, getPlatinumFile(12878), outputUri, params, true, true, true);
            runETL(engine, getPlatinumFile(12879), outputUri, params, true, true, true);




            List<String> family = Arrays.asList(father, mother, child);
            VariantStorageMetadataManager mm = engine.getMetadataManager();
            studyId = mm.getStudyId(study);
            mm.updateSampleMetadata(studyId, mm.getSampleId(studyId, child), sampleMetadata -> {
                sampleMetadata.setFather(mm.getSampleId(studyId, father));
                sampleMetadata.setMother(mm.getSampleId(studyId, mother));
                return sampleMetadata;
            });

            engine.annotate(new Query(), new ObjectMap());
            engine.fillGaps(study, family, new ObjectMap());

            VariantHbaseTestUtils.printVariants(engine.getDBAdaptor(), newOutputUri(getTestName().getMethodName()));

            stats = computeSampleVariantStatsDirectly();
        }
        VariantStorageMetadataManager mm = engine.getMetadataManager();
        List<Integer> samples = mm.getIndexedSamples(studyId);
        for (Integer sample : samples) {
            mm.updateSampleMetadata(studyId, sample, sampleMetadata -> sampleMetadata.setStats(null));
        }
    }

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(getVariantStorageEngine().getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Test
    public void testOne() throws Exception {

        ObjectMap params = new ObjectMap(SampleVariantStatsDriver.SAMPLES, father);

        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);

        List<SampleVariantStats> actualStats = readStatsFromMeta();

        Assert.assertEquals(1, actualStats.size());
        Assert.assertEquals(stats.get(0), actualStats.get(0));
    }

    @Test
    public void testAuto() throws Exception {

        // Child already has stats! Should not be calculated
        Integer childId = engine.getMetadataManager().getSampleId(studyId, child);
        engine.getMetadataManager().updateSampleMetadata(studyId, childId, sampleMetadata -> sampleMetadata.setStats(stats.get(2)));

        URI localOutputUri = newOutputUri();
        ObjectMap params = new ObjectMap().append(SampleVariantStatsDriver.SAMPLES, "auto")
                .append(SampleVariantStatsDriver.OUTPUT, localOutputUri);
        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);


        List<SampleVariantStats> actualStats = readStatsFromMeta();
        Assert.assertEquals(stats, actualStats);


        List<File> files = new ArrayList<>(FileUtils.listFiles(new File(localOutputUri), null, true));
        Assert.assertEquals(1, files.size());
        List<SampleVariantStats> statsFromFile = JacksonUtils.getDefaultObjectMapper().readerFor(SampleVariantStats.class).<SampleVariantStats>readValues(files.get(0)).readAll();
        Map<String, SampleVariantStats> statsFromFileMap = statsFromFile.stream().collect(Collectors.toMap(SampleVariantStats::getId, i -> i));
        Assert.assertEquals(stats.get(0), statsFromFileMap.get(father));
        Assert.assertEquals(stats.get(1), statsFromFileMap.get(mother));
        Assert.assertNull(statsFromFileMap.get(child));

    }

    @Test
    public void testSingleSample() throws Exception {

        URI localOutputUri = newOutputUri();
        ObjectMap params = new ObjectMap().append(SampleVariantStatsDriver.SAMPLES, mother)
                .append(SampleVariantStatsDriver.OUTPUT, localOutputUri.resolve("mother_stats.json"));
        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);


        List<SampleVariantStats> actualStats = readStatsFromMeta();

        Assert.assertEquals(1, actualStats.size());
        Assert.assertEquals(stats.get(1), actualStats.get(0));
    }

    @Test
    public void testChild() throws Exception {

        URI localOutputUri = newOutputUri();
        ObjectMap params = new ObjectMap().append(SampleVariantStatsDriver.SAMPLES, child)
                .append(SampleVariantStatsDriver.OUTPUT, localOutputUri.resolve("child_stats.json"));
        getMrExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(null, engine.getVariantTableName(), 1, null, params), params);


        List<SampleVariantStats> actualStats = readStatsFromMeta();

        // When processing a child, its parents must be processed as well
        Assert.assertEquals(3, actualStats.size());
        Assert.assertEquals(stats, actualStats);
    }

    public List<SampleVariantStats> readStatsFromMeta() throws StorageEngineException {
        List<SampleVariantStats> actualStats = new ArrayList<>(3);
        engine.getMetadataManager()
                .sampleMetadataIterator(studyId)
                .forEachRemaining(s -> {
                    if (s.getStats() != null) {
                        actualStats.add(s.getStats());
                    }
                });
        return actualStats;
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
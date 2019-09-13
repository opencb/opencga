package org.opencb.opencga.storage.core.variant.score;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantScore;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;

@Ignore
public abstract class VariantScoreLoaderTest extends VariantStorageBaseTest {

    private Set<String> scoredVariants1;
    private URI scoreFile1;
    private Set<String> scoredVariants2;
    private URI scoreFile2;

    @Before
    public void setUp() throws Exception {
        StudyMetadata studyMetadata = new StudyMetadata(1, "s1");
        runDefaultETL(smallInputUri, variantStorageEngine, studyMetadata, new QueryOptions()
                .append(VariantStorageEngine.Options.ANNOTATE.key(), false)
                .append(VariantStorageEngine.Options.CALCULATE_STATS.key(), false));
        List<String> variants = variantStorageEngine
                .stream()
                .filter(v -> !v.isSV())
                .map(Variant::toString)
                .collect(Collectors.toList());
        Collections.shuffle(variants);
        scoredVariants1 = new HashSet<>(variants.subList(0, variants.size() / 2));
        variantStorageEngine.stream().filter(Variant::isSV).map(Variant::toString).forEach(scoredVariants1::add);
        scoreFile1 = outputUri.resolve("scores1.tsv");
        generateScoreTsv1(scoreFile1, scoredVariants1);

        Collections.shuffle(variants);
        scoredVariants2 = new HashSet<>(variants.subList(0, variants.size() / 2));
        scoreFile2 = outputUri.resolve("scores2.tsv");
        generateScoreTsv2(scoreFile2, scoredVariants2);
    }

    private void generateScoreTsv1(URI scoreFile, Collection<String> scoredVariants) throws FileNotFoundException {
        try (PrintStream out = new PrintStream(new FileOutputStream(scoreFile.getPath()))) {
            out.println("#VAR\tSCORE\tP_VALUE");
            int score = 0;
            for (String scoredVariant : scoredVariants) {
                Variant v = new Variant(scoredVariant);
                out.println(tsv(v.toString(), score, score * 0.1));
                score++;
            }
        }
    }

    private void generateScoreTsv2(URI scoreFile, Collection<String> scoredVariants) throws FileNotFoundException {
        try (PrintStream out = new PrintStream(new FileOutputStream(scoreFile.getPath()))) {
            out.println("#CHR\tPOS\tREF\tALT\tSCORE\tP_VALUE");
            int score = 0;
            for (String scoredVariant : scoredVariants) {
                Variant v = new Variant(scoredVariant);
                out.println(tsv(v.getChromosome(), v.getStart(), or(v.getReference(), "-"), or(v.getAlternate(), "-"), score, score * 0.1));
                score++;
            }
        }
    }

    private String or(String s1, String s2) {
        return s1.isEmpty() ? s2 : s1;
    }

    private static String tsv(Object... objects) {
        StringBuilder sb = new StringBuilder();
        for (Object object : objects) {
            if (sb.length() > 0) {
                sb.append("\t");
            }
            sb.append(object);
        }
        return sb.toString();
    }

    @Test
    public void loadScore() throws StorageEngineException {
        variantStorageEngine.getMetadataManager().registerCohort("s1", "c1", Arrays.asList("A", "B"));
        variantStorageEngine.loadVariantScore(scoreFile1, "s1", "score1", "ALL", null, new VariantScoreFormatDescriptor(0, 1, 2), new ObjectMap());
        variantStorageEngine.loadVariantScore(scoreFile2, "s1", "score2", "c1", "ALL", new VariantScoreFormatDescriptor(0, 1, 2, 3, 4, 5), new ObjectMap());

        int varsWithScore1 = 0;
        int varsWithScore2 = 0;
        for (Variant variant : variantStorageEngine) {
            List<String> scores = variant.getStudies().get(0).getScores().stream().map(VariantScore::getId).collect(Collectors.toList());
            if (scoredVariants1.contains(variant.toString())) {
                Assert.assertThat(scores, hasItem("score1"));
                varsWithScore1++;
            }
            if (scoredVariants2.contains(variant.toString())) {
                Assert.assertThat(scores, hasItem("score2"));
                varsWithScore2++;
            }
        }
        Assert.assertEquals(scoredVariants1.size(), varsWithScore1);
        Assert.assertEquals(scoredVariants2.size(), varsWithScore2);
    }
}
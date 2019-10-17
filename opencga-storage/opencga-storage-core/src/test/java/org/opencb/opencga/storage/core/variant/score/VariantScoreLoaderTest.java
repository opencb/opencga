package org.opencb.opencga.storage.core.variant.score;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantScore;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantMatchers.*;

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
            out.println("#VAR\tSCORE");
            int score = 0;
            for (String scoredVariant : scoredVariants) {
                Variant v = new Variant(scoredVariant);
                out.println(tsv(v.toString(), score));
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
        variantStorageEngine.loadVariantScore(scoreFile1, "s1", "score1", "ALL", null, new VariantScoreFormatDescriptor(0, 1, -1), new ObjectMap());
        variantStorageEngine.loadVariantScore(scoreFile2, "s1", "score2", "c1", "ALL", new VariantScoreFormatDescriptor(0, 1, 2, 3, 4, 5), new ObjectMap());

        int varsWithScore1 = 0;
        int varsWithScore2 = 0;
        List<Variant> allVariants = variantStorageEngine.get(new Query(), new QueryOptions()).getResults();
        for (Variant variant : allVariants) {
            Map<String, VariantScore> scores = variant.getStudies().get(0).getScores().stream().collect(Collectors.toMap(VariantScore::getId, Function.identity()));
            if (scoredVariants1.contains(variant.toString())) {
                Assert.assertThat(scores.keySet(), hasItem("score1"));
                Assert.assertThat(scores.get("score1").getScore(), gte(0));
                Assert.assertTrue(Float.isNaN(scores.get("score1").getPValue()));
                varsWithScore1++;
            }
            if (scoredVariants2.contains(variant.toString())) {
                Assert.assertThat(scores.keySet(), hasItem("score2"));
                Assert.assertThat(scores.get("score2").getPValue(), gte(0));
                Assert.assertThat(scores.get("score2").getScore(), gte(0));
                varsWithScore2++;
            }
        }
        assertEquals(scoredVariants1.size(), varsWithScore1);
        assertEquals(scoredVariants2.size(), varsWithScore2);


        DataResult<Variant> result = variantStorageEngine.get(new Query(VariantQueryParam.SCORE.key(), "score1>=0"), new QueryOptions());
        assertThat(result, everyResult(allVariants, withStudy("s1", withScore("score1", gte(0)))));
        assertEquals(scoredVariants1.size(), result.getNumResults());

        result = variantStorageEngine.get(new Query(VariantQueryParam.SCORE.key(), "score1>=100"), new QueryOptions());
        assertThat(result, everyResult(allVariants, withStudy("s1", withScore("score1", gte(100)))));
        assertEquals(scoredVariants1.size() - 100, result.getNumResults());

        result = variantStorageEngine.get(new Query(VariantQueryParam.SCORE.key(), "score1>=100,score2<=100"), new QueryOptions());
        assertThat(result, everyResult(allVariants, withStudy("s1", anyOf(withScore("score1", gte(100)), withScore("score2", lte(100))))));

        result = variantStorageEngine.get(new Query(VariantQueryParam.SCORE.key(), "score1>=100;score2<=300"), new QueryOptions());
        assertThat(result, everyResult(allVariants, withStudy("s1", allOf(withScore("score1", gte(100)), withScore("score2", lte(300))))));

        result = variantStorageEngine.get(new Query(VariantQueryParam.SCORE.key(), "score1<<0;score2<<0"), new QueryOptions());
        assertThat(result, everyResult(allVariants, withStudy("s1", allOf(withScore("score1", nullValue()), withScore("score2", nullValue())))));

        result = variantStorageEngine.get(new Query(VariantQueryParam.SCORE.key(), "score1>=0;score2>=0"), new QueryOptions());
        assertThat(result, everyResult(allVariants, withStudy("s1", allOf(withScore("score1", gte(0)), withScore("score2", gte(0))))));

        // Exclude scores
        result = variantStorageEngine.get(new Query(), new QueryOptions(QueryOptions.EXCLUDE, VariantField.STUDIES_SCORES));
        assertThat(result, everyResult(withStudy("s1", allOf(withScore("score1", nullValue()), withScore("score2", nullValue())))));
    }
}
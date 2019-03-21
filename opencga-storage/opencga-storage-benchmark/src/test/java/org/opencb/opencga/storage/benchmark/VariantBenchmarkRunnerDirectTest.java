package org.opencb.opencga.storage.benchmark;

import org.junit.Test;

/**
 * Created by wasim on 05/11/18.
 */

public class VariantBenchmarkRunnerDirectTest extends VariantBenchmarkRunnerTest {

    @Test
    public void testByRegionAndProteinSubstitutionDirectAndFix() throws Exception {
        queriesTestDefault("Region", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionDirectAndRandom() throws Exception {
        queriesTestDefault("region", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByGeneDirectAndRandom() throws Exception {
        queriesTestDefault("gene", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByCTDirectAndRandom() throws Exception {
        queriesTestDefault("ct", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTypeDirectAndRandom() throws Exception {
        queriesTestDefault("type", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByStudyDirectAndRandom() throws Exception {
        queriesTestDefault("study", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByBioTypeDirectAndRandom() throws Exception {
        queriesTestDefault("biotype", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByxrefsDirectAndRandom() throws Exception {
        queriesTestDefault("xrefs", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByfileDirectAndRandom() throws Exception {
        queriesTestDefault("file", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testBysampleDirectAndRandom() throws Exception {
        queriesTestDefault("sample", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByFilterDirectAndRandom() throws Exception {
        queriesTestDefault("filter", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByDrugDirectAndRandom() throws Exception {
        queriesTestDefault("drug", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByClinicalSignificanceDirectAndRandom() throws Exception {
        queriesTestDefault("clinicalSignificance", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTranscriptFlagDirectAndRandom() throws Exception {
        queriesTestDefault("transcriptFlag", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByConservationDirectAndRandom() throws Exception {
        queriesTestDefault("conservation", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByProteinSubstitutionDirectAndRandom() throws Exception {
        queriesTestDefault("proteinSubstitution", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyAltDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyAlt", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyRefDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyRef", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyMafDirectAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyMaf", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByFunctionalScoreMafDirectAndRandom() throws Exception {
        queriesTestDefault("functionalScore", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByRegionAndBiotypeDirectAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByGeneDirectAndFixed() throws Exception {
        queriesTestDefault("Gene", 1, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }


    @Test
    public void testByRegionAndBiotypeAndGeneDirectAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype,Gene", 2, BenchmarkRunner.ConnectionType.DIRECT, BenchmarkRunner.ExecutionMode.FIXED);
    }

}

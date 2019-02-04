package org.opencb.opencga.storage.benchmark;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by wasim on 05/11/18.
 */
@Ignore
public class VariantBenchmarkRunnerRESTTest extends VariantBenchmarkRunnerTest {

    @Test
    public void testByGeneRestAndFixed() throws Exception {
        queriesTestDefault("Gene", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndProteinSubstitutionRESTAndFix() throws Exception {
        queriesTestDefault("region,proteinSubstitution(2)", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByRegionRESTAndRandom() throws Exception {
        queriesTestDefault("region", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByGeneRESTAndRandom() throws Exception {
        queriesTestDefault("gene", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByCTRESTAndRandom() throws Exception {
        queriesTestDefault("ct", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTypeRESTAndRandom() throws Exception {
        queriesTestDefault("type", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

/*
    @Test
    public void testByStudyRESTAndRandom() throws Exception {
        queriesTestDefault("study", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.REST);
    }
*/

    @Test
    public void testByBioTypeRESTAndRandom() throws Exception {
        queriesTestDefault("biotype", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByxrefsRESTAndRandom() throws Exception {
        queriesTestDefault("xrefs", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

/*
    @Test
    public void testByfileRESTAndRandom() throws Exception {
        queriesTestDefault("file", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.REST);
    }


    @Test
    public void testBysampleRESTAndRandom() throws Exception {
        queriesTestDefault("sample", BenchmarkRunner.ExecutionMode.RANDOM, BenchmarkRunner.ConnectionType.REST);
    }
*/

    @Test
    public void testByFilterRESTAndRandom() throws Exception {
        queriesTestDefault("filter", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByDrugRESTAndRandom() throws Exception {
        queriesTestDefault("drug", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByClinicalSignificanceRESTAndRandom() throws Exception {
        queriesTestDefault("clinicalSignificance", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByTranscriptionFlagRESTAndRandom() throws Exception {
        queriesTestDefault("transcriptionFlag", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByConservationRESTAndRandom() throws Exception {
        queriesTestDefault("conservation", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByProteinSubstitutionRESTAndRandom() throws Exception {
        queriesTestDefault("proteinSubstitution", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyAltRESTAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyAlt", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyRefRESTAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyRef", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByPopulationFrequencyMafRESTAndRandom() throws Exception {
        queriesTestDefault("populationFrequencyMaf", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByFunctionalScoreMafRESTAndRandom() throws Exception {
        queriesTestDefault("functionalScore(2)", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByRegionAndBiotypeRESTAndFixed() throws Exception {
        queriesTestDefault("Region", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndBiotypeRESTAndRandom() throws Exception {
        queriesTestDefault("region", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.RANDOM);
    }

    @Test
    public void testByGeneRESTAndFixed() throws Exception {
        queriesTestDefault("Gene", 1, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testByRegionAndBiotypeAndGeneRESTAndFixed() throws Exception {
        queriesTestDefault("RegionAndBiotype,Gene", 2, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

    @Test
    public void testAllRESTAndFixed() throws Exception {
        queriesTestDefault("", 4, BenchmarkRunner.ConnectionType.REST, BenchmarkRunner.ExecutionMode.FIXED);
    }

}

/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.analysis.sample.qc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.opencb.opencga.core.models.study.Variable;
import org.opencb.opencga.core.models.study.VariableSet;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class SampleSampleQcReportTest {

    @Test
    public void test() throws JsonProcessingException {

        long sampleQcVarsCounter = 0;
        Set<Variable> sampleQcVars = new LinkedHashSet<>();

        // Sample ID
        sampleQcVars.add(new Variable()
                .setName("Sample ID")
                .setId("sampleId")
                .setType(Variable.VariableType.STRING)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Sample ID"));

        // Individual ID
        sampleQcVars.add(new Variable()
                .setName("Individual ID")
                .setId("individualId")
                .setType(Variable.VariableType.STRING)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Individual ID"));

        // Father ID
        sampleQcVars.add(new Variable()
                .setName("Father ID")
                .setId("fatherId")
                .setType(Variable.VariableType.STRING)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Father ID"));

        // Mother ID
        sampleQcVars.add(new Variable()
                .setName("Mother ID")
                .setId("motherId")
                .setType(Variable.VariableType.STRING)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Mother ID"));

        // Siblings IDs
        sampleQcVars.add(new Variable()
                .setName("List of siblings IDs")
                .setId("siblingsIds")
                .setType(Variable.VariableType.STRING)
                .setMultiValue(true)
                .setRank(sampleQcVarsCounter++)
                .setDescription("List of siblings IDs"));

        //---------------------------------------------------------------------
        // Sex report
        //---------------------------------------------------------------------

        long sexReportVarsCounter = 0;
        Set<Variable> sexReportVars = new LinkedHashSet<>();

        // Sample ID
        sexReportVars.add(new Variable()
                .setName("Sample ID")
                .setId("sampleId")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Sample ID"));

        // Reported sex
        sexReportVars.add(new Variable()
                .setName("Reported sex")
                .setId("reportedSex")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Reported sex"));

        // Reported karyotypic sex
        sexReportVars.add(new Variable()
                .setName("Reported karyotypic sex")
                .setId("reportedKaryotypicSex")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Reported karyotypic sex"));

        // Ratio X
        sexReportVars.add(new Variable()
                .setName("Ratio for chromosome X")
                .setId("ratioX")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(sexReportVarsCounter++)
                .setDescription("Ratio: X-chromoxome / autosomic-chromosomes"));

        // Ratio Y
        sexReportVars.add(new Variable()
                .setName("Ratio for chromosome Y")
                .setId("ratioY")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(sexReportVarsCounter++)
                .setDescription("Ratio: Y-chromoxome / autosomic-chromosomes"));

        // Inferred karyotypic sex
        sexReportVars.add(new Variable()
                .setName("Inferred karyotypic sex")
                .setId("inferredKaryotypicSex")
                .setType(Variable.VariableType.STRING)
                .setRank(sexReportVarsCounter++)
                .setDescription("Inferred karyotypic sex"));


        // Add InferredSexReport to GenticChecksReport
        sampleQcVars.add(new Variable()
                .setName("Sex report")
                .setId("sexReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Sex report")
                .setVariableSet(sexReportVars));

        //---------------------------------------------------------------------
        // Relatedness report
        //---------------------------------------------------------------------

        long relatednessReportVarsCounter = 0;
        Set<Variable> relatednessReportVars = new LinkedHashSet<>();

        // Method
        relatednessReportVars.add(new Variable()
                .setName("Method")
                .setId("method")
                .setType(Variable.VariableType.STRING)
                .setRank(relatednessReportVarsCounter++)
                .setDescription("Method"));

        // Scores
        long scoresVarsCounter = 0;
        Set<Variable> scoresVars = new LinkedHashSet<>();

        // Sample ID #1
        scoresVars.add(new Variable()
                .setName("ID for sample #1")
                .setId("sampleId1")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Sample ID #1"));

        // Sample ID #2
        scoresVars.add(new Variable()
                .setName("ID for sample #2")
                .setId("sampleId2")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Sample ID #2"));

        // Reported relation
        scoresVars.add(new Variable()
                .setName("Reported relation")
                .setId("reportedRelation")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Reported relation"));

        // Z0
        scoresVars.add(new Variable()
                .setName("Z0 value")
                .setId("z0")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("Z0 value"));

        // Z1
        scoresVars.add(new Variable()
                .setName("Z1 value")
                .setId("z1")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("Z1 value"));

        // Z2
        scoresVars.add(new Variable()
                .setName("Z2 value")
                .setId("z2")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("Z2 value"));

        // PI-HAT
        scoresVars.add(new Variable()
                .setName("PI-HAT value")
                .setId("piHat")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(scoresVarsCounter++)
                .setDescription("PI-HAT value"));

        // Add Scores to RelatednessReport
        relatednessReportVars.add(new Variable()
                .setName("Scores")
                .setId("scores")
                .setType(Variable.VariableType.OBJECT)
                .setRank(relatednessReportVarsCounter++)
                .setDescription("Scores")
                .setVariableSet(scoresVars));

        // Add RelatednessReport to GenticChecksReport
        sampleQcVars.add(new Variable()
                .setName("Relatedness report")
                .setId("relatednessReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Relatedness report")
                .setVariableSet(relatednessReportVars));

        //---------------------------------------------------------------------
        // Mendelian errors report
        //---------------------------------------------------------------------

        long mendelianErrorsReportVarsCounter = 0;
        Set<Variable> mendelianErrorsReportVars = new LinkedHashSet<>();

        // Sample ID
        mendelianErrorsReportVars.add(new Variable()
                .setName("Sample ID")
                .setId("sampleId")
                .setType(Variable.VariableType.STRING)
                .setRank(scoresVarsCounter++)
                .setDescription("Sample ID"));

        // Number of errors for that sample
        mendelianErrorsReportVars.add(new Variable()
                .setName("Total number of errors")
                .setId("numErrors")
                .setType(Variable.VariableType.INTEGER)
                .setRank(mendelianErrorsReportVarsCounter++)
                .setDescription("Total number of errors"));

        // Error ratio
        mendelianErrorsReportVars.add(new Variable()
                .setName("Error ratio")
                .setId("errorRatio")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(mendelianErrorsReportVarsCounter++)
                .setDescription("Error ratio"));

        // Aggregation per chromosome and error code
        long chromAggregationVarsCounter = 0;
        Set<Variable> chromAggregationVars = new LinkedHashSet<>();
        chromAggregationVars.add(new Variable()
                .setName("chromosome")
                .setId("chromosome")
                .setType(Variable.VariableType.STRING)
                .setRank(chromAggregationVarsCounter++)
                .setDescription("Chromosome"));

        chromAggregationVars.add(new Variable()
                .setName("Total number of errors")
                .setId("numErrors")
                .setType(Variable.VariableType.STRING)
                .setRank(chromAggregationVarsCounter++)
                .setDescription("Total number of errors"));

        // Aggregation per error code for that chromosome
        chromAggregationVars.add(new Variable()
                .setName("Aggregation per error code")
                .setId("codeAggregation")
                .setType(Variable.VariableType.MAP_INTEGER)
                .setRank(chromAggregationVarsCounter++)
                .setDescription("Aggregation per error code for that chromosome"));

        // Aggregation per chromosome
        mendelianErrorsReportVars.add(new Variable()
                .setName("Aggregation per chromosome")
                .setId("chromAggregation")
                .setType(Variable.VariableType.OBJECT)
                .setRank(mendelianErrorsReportVarsCounter++)
                .setDescription("Aggregation per chromosome")
                .setVariableSet(chromAggregationVars));


        // Add MendelianErrorReport to GenticChecksReport
        sampleQcVars.add(new Variable()
                .setName("Mendelian errors report")
                .setId("mendelianErrorsReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Mendelian errors report")
                .setVariableSet(mendelianErrorsReportVars));

        //---------------------------------------------------------------------
        // FastQC report
        //---------------------------------------------------------------------

        long fastQcReportVarsCounter = 0;
        Set<Variable> fastQcReportVars = new LinkedHashSet<>();

        // Add FastQC to SampleQc
        sampleQcVars.add(new Variable()
                .setName("FastQC report")
                .setId("fastQcReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(sampleQcVarsCounter++)
                .setDescription("FastQC report (from the FastQC tool)")
                .setVariableSet(fastQcReportVars));

        //---------------------------------------------------------------------
        // Flag stats report
        //---------------------------------------------------------------------

        long flagStatsReportVarsCounter = 0;
        Set<Variable> flagStatsReportVars = new LinkedHashSet<>();

        // Total reads
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads")
                .setId("totalReads")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads"));

        // Total QC passed
        flagStatsReportVars.add(new Variable()
                .setName("Total QC passed")
                .setId("totalQcPassed")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads marked as not passing quality controls"));

        // Mapped
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads which are mapped")
                .setId("mapped")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads which are mapped (0x4 bit not set)"));


        // Secondary alignments
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads which are secondary")
                .setId("secondaryAlignments")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads which are secondary (0x100 bit set)"));

        // Duplicates
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads which are duplicates")
                .setId("duplicates")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads which are duplicates (0x400 bit set)"));

        // Paired in sequencing
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads which are paired in sequencing")
                .setId("pairedInSequencing")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads which are paired in sequencing (0x1 bit set)"));

        // Properly paired
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads which are properly paired")
                .setId("properlyPaired")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads which are properly paired (both 0x1 and 0x2 bits set and 0x4 bit not set)"));

        // Self and mate mapped
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads with itself and mate mapped")
                .setId("selfAndMateMapped")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads with itself and mate mapped (0x1 bit set and neither 0x4 nor 0x8 bits set)"));

        // Singletons
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads which are singletons")
                .setId("singletons")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads which are singletons (both 0x1 and 0x8 bits set and bit 0x4 not set)"));

        // Mate mapped to different chromosome
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads with mate mapped to a different chromosome")
                .setId("mateMappedToDiffChr")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads with mate mapped to a different chromosome (0x1 bit set and neither 0x4 nor 0x8"
                        + " bits set and MRNM not equal to RNAME)"));

        // Mate mapped to different chromosome with mapping quality at least 5 (mapQ>=5)
        flagStatsReportVars.add(new Variable()
                .setName("Total number of reads with mate mapped to a different chromosome and with mapping quality at least 5")
                .setId("diffChrMapQ5")
                .setType(Variable.VariableType.INTEGER)
                .setRank(flagStatsReportVarsCounter++)
                .setDescription("Total number of reads with mate mapped to a different chromosome and with mapping quality at least 5"
                        + " (0x1 bit set and neither 0x4 nor 0x8 bits set and MRNM not equal to RNAME and MAPQ >= 5)"));


        // Add FlagStatsReport to SampleQc
        sampleQcVars.add(new Variable()
                .setName("Flag stats report")
                .setId("flagStatsReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Flag stats report (from the samtools/flagstat command)")
                .setVariableSet(flagStatsReportVars));

        //---------------------------------------------------------------------
        // Hs metrics report
        //---------------------------------------------------------------------

        long hsMetricsReportVarsCounter = 0;
        Set<Variable> hsMetricsReportVars = new LinkedHashSet<>();

        // At dropout
        hsMetricsReportVars.add(new Variable()
                .setName("A measure of how undercovered <= 50% GC regions are relative to the mean")
                .setId("atDropout")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("A measure of how undercovered <= 50% GC regions are relative to the mean"));

        // Bait design efficiency
        hsMetricsReportVars.add(new Variable()
                .setName("Bait design efficiency")
                .setId("baitDesignEfficiency")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("Target terrirtoy / bait territory. 1 == perfectly efficient, 0.5 = half of baited bases are not target"));

        // Bait territory
        hsMetricsReportVars.add(new Variable()
                .setName("Bait territory")
                .setId("baitTerritory")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of bases which have one or more baits on top of them"));

        // Fold 80 base penalty
        hsMetricsReportVars.add(new Variable()
                .setName("Fold 80 base penalty")
                .setId("fold80BasePenalty")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The fold over-coverage necessary to raise 80% of bases in \"non-zero-cvg\" targets to the mean coverage"
                        + " level in those targets"));

        // Fold enrichment
        hsMetricsReportVars.add(new Variable()
                .setName("Fold enrichment")
                .setId("foldEnrichment")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The fold by which the baited region has been amplified above genomic background"));

        // Gc dropout
        hsMetricsReportVars.add(new Variable()
                .setName("Gc dropout")
                .setId("gcDropout")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("A measure of how undercovered >= 50% GC regions are relative to the mean"));

        // Het snp q
        hsMetricsReportVars.add(new Variable()
                .setName("Het snp q")
                .setId("hetSnpQ")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The Phred Scaled Q Score of the theoretical HET SNP sensitivity"));

        // Het snp sensitivity
        hsMetricsReportVars.add(new Variable()
                .setName("Het snp sensitivity")
                .setId("hetSnpSensitivity")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The theoretical HET SNP sensitivity"));

        // Near bait bases
        hsMetricsReportVars.add(new Variable()
                .setName("Near bait bases")
                .setId("nearBaitBases")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF aligned bases that mapped to within a fixed interval of a baited region, but not on a"
                        + " baited region"));

        // Off bait bases
        hsMetricsReportVars.add(new Variable()
                .setName("Off bait bases")
                .setId("offBaitBases")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF aligned bases that mapped to neither on or near a bait"));

        // On bait bases
        hsMetricsReportVars.add(new Variable()
                .setName("On bait bases")
                .setId("onBaitBases")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF aligned bases that mapped to a baited region of the genome"));

        // On target bases
        hsMetricsReportVars.add(new Variable()
                .setName("On target bases")
                .setId("onTargetBases")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF aligned bases that mapped to a targeted region of the genome"));

        // Pf bases
        hsMetricsReportVars.add(new Variable()
                .setName("Pf bases")
                .setId("pfBases")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("Pf bases"));

        // Pf bases aligned
        hsMetricsReportVars.add(new Variable()
                .setName("Pf bases aligned")
                .setId("pfBasesAligned")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF unique bases that are aligned with mapping score > 0 to the reference"
                        + " genome"));

        // Pf reads
        hsMetricsReportVars.add(new Variable()
                .setName("Pf reads")
                .setId("pfReads")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of reads that pass the vendor's filter"));

        // Pf unique reads
        hsMetricsReportVars.add(new Variable()
                .setName("Pf unique reads")
                .setId("pfUniqueReads")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF reads that are not marked as duplicates"));

        // Pf uq bases aligned
        hsMetricsReportVars.add(new Variable()
                .setName("Pf uq bases aligned")
                .setId("pfUqBasesAligned")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of bases in the PF aligned reads that are mapped to a reference base. Accounts for clipping"
                        + " and gaps"));

        // Pf uq reads aligned
        hsMetricsReportVars.add(new Variable()
                .setName("Pf uq reads aligned")
                .setId("pfUqReadsAligned")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of PF unique reads that are aligned with mapping score > 0 to the reference genome"));

        // Total reads
        hsMetricsReportVars.add(new Variable()
                .setName("Total reads")
                .setId("totalReads")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The total number of reads in the SAM or BAM file examine"));

        // Max target coverage
        hsMetricsReportVars.add(new Variable()
                .setName("Max target coverage")
                .setId("maxTargetCoverage")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("Max target coverage"));

        // Mean bait coverage
        hsMetricsReportVars.add(new Variable()
                .setName("Mean bait coverage")
                .setId("meanBaitCoverage")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The mean coverage of all baits in the experiment"));

        // Mean target coverage
        hsMetricsReportVars.add(new Variable()
                .setName("Mean target coverage")
                .setId("meanTargetCoverage")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The mean coverage of targets"));

        // Median target coverage
        hsMetricsReportVars.add(new Variable()
                .setName("Median target coverage")
                .setId("medianTargetCoverage")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The median coverage of targets"));

        // Min target coverage
        hsMetricsReportVars.add(new Variable()
                .setName("Min target coverage")
                .setId("minTargetCoverage")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The minimum coverage of targets"));

        // On bait vs selected
        hsMetricsReportVars.add(new Variable()
                .setName("On bait vs selected")
                .setId("onBaitVsSelected")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The percentage of on+near bait bases that are on as opposed to near"));

        // Target territory
        hsMetricsReportVars.add(new Variable()
                .setName("Target territory")
                .setId("targetTerritory")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The unique number of target bases in the experiment where target is usually exons etc."));

        // Zero cvg targets pct
        hsMetricsReportVars.add(new Variable()
                .setName("Zero cvg targets pct")
                .setId("zeroCvgTargetsPct")
                .setType(Variable.VariableType.DOUBLE)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The fraction of targets that did not reach coverage=1 over any base"));

        // Zero cvg targets pct
        hsMetricsReportVars.add(new Variable()
                .setName("Zero cvg targets pct")
                .setId("zeroCvgTargetsPct")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The fraction of targets that did not reach coverage=1 over any base"));

        // HS library size
        hsMetricsReportVars.add(new Variable()
                .setName("Hs library size")
                .setId("hsLibrarySize")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The estimated number of unique molecules in the selected part of the library"));

        // Bait set
        hsMetricsReportVars.add(new Variable()
                .setName("Bait set")
                .setId("baitSet")
                .setType(Variable.VariableType.STRING)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The name of the bait set used in the hybrid selection"));

        // Genome size
        hsMetricsReportVars.add(new Variable()
                .setName("Genome size")
                .setId("Genome size")
                .setType(Variable.VariableType.INTEGER)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The number of bases in the reference genome used for alignment"));

// PCT_EXC_DUPE	The fraction of aligned bases that were filtered out because they were in reads marked as duplicates.
// PCT_EXC_MAPQ	The fraction of aligned bases that were filtered out because they were in reads with low mapping quality.
// PCT_EXC_BASEQ	The fraction of aligned bases that were filtered out because they were of low base quality.
// PCT_EXC_OVERLAP	The fraction of aligned bases that were filtered out because they were the second observation from an insert with overlapping reads.
// PCT_EXC_OFF_TARGET	The fraction of aligned bases that were filtered out because they did not align over a target base.

        // Pct target bases 1x, 2x, 10x, 20x, 30x, 40x, 50x, 100x
        hsMetricsReportVars.add(new Variable()
                .setName("Pct target bases 1x, 2x, 10x, 20x, 30x, 40x, 50x, 100x")
                .setId("pctTargetBases")
                .setType(Variable.VariableType.DOUBLE)
                .setMultiValue(true)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The fraction of all target bases achieving 1x, 2x, 10x, 20x, 30x, 40x, 50x and 100x"));

        // Hybrid selection penalty to 10x, 20x, 30x, 40x, 50x, 100x
        hsMetricsReportVars.add(new Variable()
                .setName("Hybrid selection penalty to 10x, 20x, 30x, 40x, 50x, 100x")
                .setId("hsPenalty")
                .setType(Variable.VariableType.DOUBLE)
                .setMultiValue(true)
                .setRank(hsMetricsReportVarsCounter++)
                .setDescription("The hybrid selection penalty incurred to get 80% of target bases to 10X, 20x, 30x, 40x, 50x and"
                        + " 100x"));

        // Add HsMetricsReport to SampleQc
        sampleQcVars.add(new Variable()
                .setName("Hs metrics report")
                .setId("hsMetricsReport")
                .setType(Variable.VariableType.OBJECT)
                .setRank(sampleQcVarsCounter++)
                .setDescription("Hs metrics report (from the picard/CollecHsMetrics command)")
                .setVariableSet(hsMetricsReportVars));

        //---------------------------------------------------------------------
        // Sample QC variable set
        //---------------------------------------------------------------------

        // Variable set
        VariableSet sampleQcVs = new VariableSet()
                .setId("opencga_sample_qc")
                .setName("opencga_sample_qc")
                .setDescription("OpenCGA variable set for sample quality control (QC)")
                .setUnique(true)
                .setEntities(Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE))
                .setVariables(sampleQcVars);

        // Generate JSON
        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sampleQcVs));
    }
}

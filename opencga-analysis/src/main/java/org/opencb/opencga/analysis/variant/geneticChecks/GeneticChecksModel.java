package org.opencb.opencga.analysis.variant.geneticChecks;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class GeneticChecksModel implements Serializable {

    // Sample ID
    private String sampleId;

    // Father, mother and siblling IDs
    private String fatherId;
    private String motherId;
    private List<String> siblingIds;

    // Sex report
    private SexReport sexReport;

    // Relatedness report
    private RelatednessReport relatednessRepoort;

    // Mendelian errors report
    private MendelianErrorsReport mendelianErrorsReport;

    //-------------------------------------------------------------------------
    // S E X     R E P O R T
    //-------------------------------------------------------------------------

    public class SexReport {

        // Reported values
        private String reportedSex;
        private String reportedKaryotypicSex;

        // Ratio: X-chrom / autosoma-chroms
        private double ratioX;

        // Ratio: Y-chrom / autosoma-chroms
        private double ratioY;

        // Inferred karyotypic sex
        private String inferredKaryotypicSex;

        public SexReport() {
        }

        public SexReport(String reportedSex, String reportedKaryotypicSex, double ratioX, double ratioY, String inferredKaryotypicSex) {
            this.reportedSex = reportedSex;
            this.reportedKaryotypicSex = reportedKaryotypicSex;
            this.ratioX = ratioX;
            this.ratioY = ratioY;
            this.inferredKaryotypicSex = inferredKaryotypicSex;
        }

        public String getReportedSex() {
            return reportedSex;
        }

        public SexReport setReportedSex(String reportedSex) {
            this.reportedSex = reportedSex;
            return this;
        }

        public String getReportedKaryotypicSex() {
            return reportedKaryotypicSex;
        }

        public SexReport setReportedKaryotypicSex(String reportedKaryotypicSex) {
            this.reportedKaryotypicSex = reportedKaryotypicSex;
            return this;
        }

        public double getRatioX() {
            return ratioX;
        }

        public SexReport setRatioX(double ratioX) {
            this.ratioX = ratioX;
            return this;
        }

        public double getRatioY() {
            return ratioY;
        }

        public SexReport setRatioY(double ratioY) {
            this.ratioY = ratioY;
            return this;
        }

        public String getInferredKaryotypicSex() {
            return inferredKaryotypicSex;
        }

        public SexReport setInferredKaryotypicSex(String inferredKaryotypicSex) {
            this.inferredKaryotypicSex = inferredKaryotypicSex;
            return this;
        }
    }

    //-------------------------------------------------------------------------
    // R E L A T E D N E S S     R E P O R T
    //-------------------------------------------------------------------------

    public class RelatednessReport {

        // Method., e.g.: IBD
        private String method;

        // Relatedness scores for pair of samples
        private List<RelatednessScore> scores;

        //-------------------------------------------------------------------------
        // R E L A T E D N E S S     S C O R E
        //-------------------------------------------------------------------------

        public class RelatednessScore {
            // Pair of samples
            private String sampleId1;
            private String sampleId2;

            // Reported relation according to pedigree
            private String reportedRelation;

            // Z scores
            private double z0;
            private double z1;
            private double z2;

            // PI-HAT score
            private double piHat;

            public RelatednessScore() {
            }

            public RelatednessScore(String sampleId1, String sampleId2, String reportedRelation, double z0, double z1, double z2, double piHat)
            {
                this.sampleId1 = sampleId1;
                this.sampleId2 = sampleId2;
                this.reportedRelation = reportedRelation;
                this.z0 = z0;
                this.z1 = z1;
                this.z2 = z2;
                this.piHat = piHat;
            }

            public String getSampleId1() {
                return sampleId1;
            }

            public RelatednessScore setSampleId1(String sampleId1) {
                this.sampleId1 = sampleId1;
                return this;
            }

            public String getSampleId2() {
                return sampleId2;
            }

            public RelatednessScore setSampleId2(String sampleId2) {
                this.sampleId2 = sampleId2;
                return this;
            }

            public String getReportedRelation() {
                return reportedRelation;
            }

            public RelatednessScore setReportedRelation(String reportedRelation) {
                this.reportedRelation = reportedRelation;
                return this;
            }

            public double getZ0() {
                return z0;
            }

            public RelatednessScore setZ0(double z0) {
                this.z0 = z0;
                return this;
            }

            public double getZ1() {
                return z1;
            }

            public RelatednessScore setZ1(double z1) {
                this.z1 = z1;
                return this;
            }

            public double getZ2() {
                return z2;
            }

            public RelatednessScore setZ2(double z2) {
                this.z2 = z2;
                return this;
            }

            public double getPiHat() {
                return piHat;
            }

            public RelatednessScore setPiHat(double piHat) {
                this.piHat = piHat;
                return this;
            }
        }

        public RelatednessReport() {
        }

        public RelatednessReport(String method, List<RelatednessScore> scores) {
            this.method = method;
            this.scores = scores;
        }

        public String getMethod() {
            return method;
        }

        public RelatednessReport setMethod(String method) {
            this.method = method;
            return this;
        }

        public List<RelatednessScore> getScores() {
            return scores;
        }

        public RelatednessReport setScores(List<RelatednessScore> scores) {
            this.scores = scores;
            return this;
        }
    }

    //-------------------------------------------------------------------------
    // M E N D E L I A N     E R R O R S     R E P O R T
    //-------------------------------------------------------------------------

    public class MendelianErrorsReport {

        // Number total of errors for that sample
        private int numErrors;

        // Error ratio for that sample = total / number_of_variants
        private double ratio;

        // Aggregation per chromosome
        private ChromosomeAggregation chromAggregation;

        //-------------------------------------------------------------------------
        // C H R O M O S O M E     A G G R E G A T I O N
        //-------------------------------------------------------------------------

        public class ChromosomeAggregation {

            // Chromosome
            private String chromosome;

            // Number of errors
            private int numErrors;

            // Aggregation per error code for that chromosome
            private Map<String, Integer> errorCodeAggregation;

            public ChromosomeAggregation() {
            }

            public ChromosomeAggregation(String chromosome, int numErrors, Map<String, Integer> errorCodeAggregation) {
                this.chromosome = chromosome;
                this.numErrors = numErrors;
                this.errorCodeAggregation = errorCodeAggregation;
            }

            public String getChromosome() {
                return chromosome;
            }

            public ChromosomeAggregation setChromosome(String chromosome) {
                this.chromosome = chromosome;
                return this;
            }

            public int getNumErrors() {
                return numErrors;
            }

            public ChromosomeAggregation setNumErrors(int numErrors) {
                this.numErrors = numErrors;
                return this;
            }

            public Map<String, Integer> getErrorCodeAggregation() {
                return errorCodeAggregation;
            }

            public ChromosomeAggregation setErrorCodeAggregation(Map<String, Integer> errorCodeAggregation) {
                this.errorCodeAggregation = errorCodeAggregation;
                return this;
            }
        }

        public MendelianErrorsReport() {
        }

        public MendelianErrorsReport(int numErrors, double ratio, ChromosomeAggregation chromAggregation) {
            this.numErrors = numErrors;
            this.ratio = ratio;
            this.chromAggregation = chromAggregation;
        }

        public int getNumErrors() {
            return numErrors;
        }

        public MendelianErrorsReport setNumErrors(int numErrors) {
            this.numErrors = numErrors;
            return this;
        }

        public double getRatio() {
            return ratio;
        }

        public MendelianErrorsReport setRatio(double ratio) {
            this.ratio = ratio;
            return this;
        }

        public ChromosomeAggregation getChromAggregation() {
            return chromAggregation;
        }

        public MendelianErrorsReport setChromAggregation(ChromosomeAggregation chromAggregation) {
            this.chromAggregation = chromAggregation;
            return this;
        }
    }

    public GeneticChecksModel() {
    }

    public GeneticChecksModel(String sampleId, String fatherId, String motherId, List<String> siblingIds, SexReport sexReport,
                              RelatednessReport relatednessRepoort, MendelianErrorsReport mendelianErrorsReport) {
        this.sampleId = sampleId;
        this.fatherId = fatherId;
        this.motherId = motherId;
        this.siblingIds = siblingIds;
        this.sexReport = sexReport;
        this.relatednessRepoort = relatednessRepoort;
        this.mendelianErrorsReport = mendelianErrorsReport;
    }

    public String getSampleId() {
        return sampleId;
    }

    public GeneticChecksModel setSampleId(String sampleId) {
        this.sampleId = sampleId;
        return this;
    }

    public String getFatherId() {
        return fatherId;
    }

    public GeneticChecksModel setFatherId(String fatherId) {
        this.fatherId = fatherId;
        return this;
    }

    public String getMotherId() {
        return motherId;
    }

    public GeneticChecksModel setMotherId(String motherId) {
        this.motherId = motherId;
        return this;
    }

    public List<String> getSiblingIds() {
        return siblingIds;
    }

    public GeneticChecksModel setSiblingIds(List<String> siblingIds) {
        this.siblingIds = siblingIds;
        return this;
    }

    public SexReport getSexReport() {
        return sexReport;
    }

    public GeneticChecksModel setSexReport(SexReport sexReport) {
        this.sexReport = sexReport;
        return this;
    }

    public RelatednessReport getRelatednessRepoort() {
        return relatednessRepoort;
    }

    public GeneticChecksModel setRelatednessRepoort(RelatednessReport relatednessRepoort) {
        this.relatednessRepoort = relatednessRepoort;
        return this;
    }

    public MendelianErrorsReport getMendelianErrorsReport() {
        return mendelianErrorsReport;
    }

    public GeneticChecksModel setMendelianErrorsReport(MendelianErrorsReport mendelianErrorsReport) {
        this.mendelianErrorsReport = mendelianErrorsReport;
        return this;
    }
}

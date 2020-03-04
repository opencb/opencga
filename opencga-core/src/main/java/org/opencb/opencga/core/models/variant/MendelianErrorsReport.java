package org.opencb.opencga.core.models.variant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//-------------------------------------------------------------------------
// M E N D E L I A N     E R R O R S     R E P O R T
//-------------------------------------------------------------------------

public class MendelianErrorsReport {

    // Number total of errors for that family
    private int numErrors;

    private List<SampleAggregation> sampleAggregation;

    //-------------------------------------------------------------------------
    // S A M P L E     A G G R E G A T I O N
    //-------------------------------------------------------------------------

    public static class SampleAggregation {

        // Sample
        private String sample;

        // Number of errors
        private int numErrors;

        // Ratio for that sample = total / number_of_variants
        private double ratio;


        // Aggregation per chromosome
        private List<ChromosomeAggregation> chromAggregation;

        public SampleAggregation() {
            chromAggregation = new ArrayList<>();
        }

        public SampleAggregation(String sample, int numErrors, double ratio, List<ChromosomeAggregation> chromAggregation) {
            this.sample = sample;
            this.numErrors = numErrors;
            this.ratio = ratio;
            this.chromAggregation = chromAggregation;
        }

        public String getSample() {
            return sample;
        }

        public SampleAggregation setSample(String sample) {
            this.sample = sample;
            return this;
        }

        public int getNumErrors() {
            return numErrors;
        }

        public SampleAggregation setNumErrors(int numErrors) {
            this.numErrors = numErrors;
            return this;
        }

        public double getRatio() {
            return ratio;
        }

        public SampleAggregation setRatio(double ratio) {
            this.ratio = ratio;
            return this;
        }

        public List<ChromosomeAggregation> getChromAggregation() {
            return chromAggregation;
        }

        public SampleAggregation setChromAggregation(List<ChromosomeAggregation> chromAggregation) {
            this.chromAggregation = chromAggregation;
            return this;
        }

        //-------------------------------------------------------------------------
        // C H R O M O S O M E     A G G R E G A T I O N
        //-------------------------------------------------------------------------

        public static class ChromosomeAggregation {

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
    }

    public MendelianErrorsReport() {
        this.numErrors = 0;
        this.sampleAggregation = new ArrayList<>();
    }

    public MendelianErrorsReport(int numErrors, List<SampleAggregation> sampleAggregation) {
        this.numErrors = numErrors;
        this.sampleAggregation = sampleAggregation;
    }

    public int getNumErrors() {
        return numErrors;
    }

    public MendelianErrorsReport setNumErrors(int numErrors) {
        this.numErrors = numErrors;
        return this;
    }

    public List<SampleAggregation> getSampleAggregation() {
        return sampleAggregation;
    }

    public MendelianErrorsReport setSampleAggregation(List<SampleAggregation> sampleAggregation) {
        this.sampleAggregation = sampleAggregation;
        return this;
    }
}

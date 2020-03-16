package org.opencb.opencga.core.models.variant;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RelatednessReport {

    // Method., e.g.: IBD
    private String method;

    // Relatedness scores for pair of samples
    private List<RelatednessScore> scores;

    //-------------------------------------------------------------------------
    // R E L A T E D N E S S     S C O R E
    //-------------------------------------------------------------------------

    public static class RelatednessScore {
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
        this.method = "IBD";
        this.scores = new ArrayList<>();
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

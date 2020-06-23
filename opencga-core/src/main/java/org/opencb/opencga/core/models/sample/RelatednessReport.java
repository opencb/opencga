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

package org.opencb.opencga.core.models.sample;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RelatednessReport {

    // Method., e.g.: IBD
    private String method;

    // Relatedness scores for pair of samples
    private List<RelatednessScore> scores;
    private List<String> files;

    public RelatednessReport() {
        this("Plink/IBD", new ArrayList<>(), new ArrayList<>());
    }

    public RelatednessReport(String method, List<RelatednessScore> scores, List<String> files) {
        this.method = method;
        this.scores = scores;
        this.files = files;
    }

    public static class RelatednessScore {
        // Pair of samples
        private String sampleId1;
        private String sampleId2;

        // Reported relation according to pedigree
        private String inferredRelationship;

        private Map<String, Object> values = new LinkedHashMap<>();
        private double z0;
        private double z1;
        private double z2;
        private double piHat;

        public RelatednessScore() {
        }

        public RelatednessScore(String sampleId1, String sampleId2, String inferredRelationship, double z0, double z1, double z2, double piHat)
        {
            this.sampleId1 = sampleId1;
            this.sampleId2 = sampleId2;
            this.inferredRelationship = inferredRelationship;
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

        public String getInferredRelationship() {
            return inferredRelationship;
        }

        public RelatednessScore setInferredRelationship(String inferredRelationship) {
            this.inferredRelationship = inferredRelationship;
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


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RelatednessReport{");
        sb.append("method='").append(method).append('\'');
        sb.append(", scores=").append(scores);
        sb.append('}');
        return sb.toString();
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

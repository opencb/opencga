package org.opencb.opencga.analysis.variant.samples;

import org.opencb.biodata.models.pedigree.IndividualProperty;

import java.util.List;

public class SampleEligibilityAnalysisResult {

    private String query;
    private TreeQuery.Node queryPlan;
    private String date;
    private String study;
    private int numSamples;
    private List<ElectedIndividual> individuals;

    public String getQuery() {
        return query;
    }

    public SampleEligibilityAnalysisResult setQuery(String query) {
        this.query = query;
        return this;
    }

    public TreeQuery.Node getQueryPlan() {
        return queryPlan;
    }

    public SampleEligibilityAnalysisResult setQueryPlan(TreeQuery.Node queryPlan) {
        this.queryPlan = queryPlan;
        return this;
    }

    public String getDate() {
        return date;
    }

    public SampleEligibilityAnalysisResult setDate(String date) {
        this.date = date;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public SampleEligibilityAnalysisResult setStudy(String study) {
        this.study = study;
        return this;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public SampleEligibilityAnalysisResult setNumSamples(int numSamples) {
        this.numSamples = numSamples;
        return this;
    }

    public List<ElectedIndividual> getIndividuals() {
        return individuals;
    }

    public SampleEligibilityAnalysisResult setIndividuals(List<ElectedIndividual> individuals) {
        this.individuals = individuals;
        return this;
    }

    public static class ElectedIndividual {

        private String id;
        private String name;
        private IndividualProperty.Sex sex;
        private List<String> disorders;
        private List<String> phenotypes;
        private SampleSummary sample;

        public String getId() {
            return id;
        }

        public ElectedIndividual setId(String id) {
            this.id = id;
            return this;
        }

        public String getName() {
            return name;
        }

        public ElectedIndividual setName(String name) {
            this.name = name;
            return this;
        }

        public IndividualProperty.Sex getSex() {
            return sex;
        }

        public ElectedIndividual setSex(IndividualProperty.Sex sex) {
            this.sex = sex;
            return this;
        }

        public List<String> getDisorders() {
            return disorders;
        }

        public ElectedIndividual setDisorders(List<String> disorders) {
            this.disorders = disorders;
            return this;
        }

        public List<String> getPhenotypes() {
            return phenotypes;
        }

        public ElectedIndividual setPhenotypes(List<String> phenotypes) {
            this.phenotypes = phenotypes;
            return this;
        }

        public SampleSummary getSample() {
            return sample;
        }

        public ElectedIndividual setSample(SampleSummary sample) {
            this.sample = sample;
            return this;
        }
    }

    public static class SampleSummary {
        private String id;
        private String creationDate;
        private boolean somatic;

        public String getId() {
            return id;
        }

        public SampleSummary setId(String id) {
            this.id = id;
            return this;
        }

        public String getCreationDate() {
            return creationDate;
        }

        public SampleSummary setCreationDate(String creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        public boolean isSomatic() {
            return somatic;
        }

        public SampleSummary setSomatic(boolean somatic) {
            this.somatic = somatic;
            return this;
        }
    }

}

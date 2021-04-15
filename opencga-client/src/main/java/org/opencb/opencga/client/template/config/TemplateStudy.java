package org.opencb.opencga.client.template.config;

import org.opencb.opencga.core.models.clinical.ClinicalAnalysisCreateParams;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.panel.PanelCreateParams;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.study.StudyCreateParams;
import org.opencb.opencga.core.models.study.StudyVariantEngineConfiguration;
import org.opencb.opencga.core.models.study.VariableSetCreateParams;

import java.util.List;

public class TemplateStudy extends StudyCreateParams {

    private List<SampleCreateParams> samples;
    private List<IndividualCreateParams> individuals;
    private List<FamilyCreateParams> families;
    private List<CohortCreateParams> cohorts;
    private List<PanelCreateParams> panels;
    private List<ClinicalAnalysisCreateParams> clinicalAnalyses;
    private List<VariableSetCreateParams> variableSets;
    private List<TemplateFile> files;

//    private StudyVariantEngineConfiguration variantEngineConfiguration;

    public List<SampleCreateParams> getSamples() {
        return samples;
    }

    public TemplateStudy setSamples(List<SampleCreateParams> samples) {
        this.samples = samples;
        return this;
    }

    public List<IndividualCreateParams> getIndividuals() {
        return individuals;
    }

    public TemplateStudy setIndividuals(List<IndividualCreateParams> individuals) {
        this.individuals = individuals;
        return this;
    }

    public List<FamilyCreateParams> getFamilies() {
        return families;
    }

    public TemplateStudy setFamilies(List<FamilyCreateParams> families) {
        this.families = families;
        return this;
    }

    public List<CohortCreateParams> getCohorts() {
        return cohorts;
    }

    public TemplateStudy setCohorts(List<CohortCreateParams> cohorts) {
        this.cohorts = cohorts;
        return this;
    }

    public List<PanelCreateParams> getPanels() {
        return panels;
    }

    public TemplateStudy setPanels(List<PanelCreateParams> panels) {
        this.panels = panels;
        return this;
    }

    public List<ClinicalAnalysisCreateParams> getClinicalAnalyses() {
        return clinicalAnalyses;
    }

    public TemplateStudy setClinicalAnalyses(List<ClinicalAnalysisCreateParams> clinicalAnalyses) {
        this.clinicalAnalyses = clinicalAnalyses;
        return this;
    }

    public List<VariableSetCreateParams> getVariableSets() {
        return variableSets;
    }

    public TemplateStudy setVariableSets(List<VariableSetCreateParams> variableSets) {
        this.variableSets = variableSets;
        return this;
    }

    public List<TemplateFile> getFiles() {
        return files;
    }

    public TemplateStudy setFiles(List<TemplateFile> files) {
        this.files = files;
        return this;
    }
}

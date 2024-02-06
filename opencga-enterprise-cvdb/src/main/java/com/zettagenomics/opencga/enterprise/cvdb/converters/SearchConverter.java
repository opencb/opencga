package com.zettagenomics.opencga.enterprise.cvdb.converters;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class SearchConverter<M, N> {

    public static final String CVDB_INTERNALS_KEY = "OPENCGA_CVDB_INTERNALS";
    public static final String CVDB_VIEWERS_KEY = "OPENCGA_VIEWERS";
    public static final String CVDB_STUDY_ID_KEY = "OPENCGA_STUDY_ID";

    protected static final String UP_FIELD_SEPARATOR = "===";
    protected static final String FIELD_SEPARATOR = "---";
    protected static final String EMPTY_VALUE = "**";

    protected ObjectMapper mapper;
    protected ObjectReader mapReader;

    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    public static SimpleDateFormat solrDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    public SearchConverter() {
        this.mapper = JacksonUtils.getDefaultObjectMapper();
        this.mapReader = mapper.readerFor(HashMap.class);
    }

    public M toModel(N input) throws CvdbException {
        return null;
    }

    protected void minimizePanels(List<Panel> panels) {
        for (Panel panel : panels) {
            panel.setVariants(null);
            panel.setStrs(null);
            panel.setGenes(null);
            panel.setRegions(null);
        }
    }

    protected Individual getMinimizedIndividual(Individual oldIndividual) {
        Individual newIndividual = new Individual()
                .setId(oldIndividual.getId())
                .setName(oldIndividual.getName())
                .setSex(oldIndividual.getSex());
        if (CollectionUtils.isNotEmpty(oldIndividual.getSamples())) {
            List<Sample> newSamples = oldIndividual.getSamples().stream().map(s -> new Sample().setId(s.getId()))
                    .collect(Collectors.toList());
            newIndividual.setSamples(newSamples);
        }
        return newIndividual;
    }

    protected Family getMinimizedFamily(Family oldFamily) {
        Family newFamily = new Family()
                .setId(oldFamily.getId())
                .setName(oldFamily.getName());
        if (CollectionUtils.isNotEmpty(oldFamily.getMembers())) {
            List<Individual> newMembers = oldFamily.getMembers().stream().map(m -> getMinimizedIndividual(m)).collect(Collectors.toList());
            newFamily.setMembers(newMembers);
        }
        return newFamily;
    }

    protected Panel getMinimizedPanel(Panel oldPanel) {
        Panel newPanel = new Panel();
        newPanel.setId(oldPanel.getId());
        newPanel.setName(oldPanel.getName());
        newPanel.setSource(oldPanel.getSource());
        newPanel.setStats(oldPanel.getStats());
        return newPanel;
    }

    protected void minimizeClinicalVariants(List<ClinicalVariant> cvs) {
        if (CollectionUtils.isNotEmpty(cvs)) {
            for (ClinicalVariant cv : cvs) {
                cv.setAnnotation(null);
                for (ClinicalVariantEvidence evidence : cv.getEvidences()) {
                    evidence.setGenomicFeature(null);
                    evidence.setAttributes(null);
                    evidence.setModeOfInheritances(null);
                    evidence.setReview(null);
                    evidence.setCompoundHeterozygousVariantIds(null);
                    evidence.setPhenotypes(null);
                    evidence.setRolesInCancer(null);
                    evidence.setClassification(null);
                    evidence.setInterpretationMethodName(null);
                    evidence.setPenetrance(null);
                    evidence.setPanelId(null);
                }
//                cv.setEvidences(null);
            }
        }
    }
}

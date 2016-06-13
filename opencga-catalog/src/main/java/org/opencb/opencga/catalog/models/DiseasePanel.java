package org.opencb.opencga.catalog.models;

import org.opencb.opencga.catalog.models.acls.DiseasePanelAcl;

import java.util.Collections;
import java.util.List;

/**
 * Created by pfurio on 01/06/16.
 */
public class DiseasePanel {

    private long id;

    private String name;
    private String disease;
    private String description;

    private List<String> genes;
    private List<String> regions;
    private List<String> variants;

    private PanelStatus status;
    private List<DiseasePanelAcl> acls;

    public DiseasePanel() {
    }

    public DiseasePanel(long id, String name, String disease, String description, List<String> genes, List<String> regions,
                        List<String> variants, PanelStatus status) {
        this.id = id;
        this.name = name;
        this.disease = disease;
        this.description = description;
        this.genes = genes;
        this.regions = regions;
        this.variants = variants;
        this.acls = Collections.emptyList();
        this.status = status;
    }

    public long getId() {
        return id;
    }

    public DiseasePanel setId(long id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public DiseasePanel setName(String name) {
        this.name = name;
        return this;
    }

    public String getDisease() {
        return disease;
    }

    public DiseasePanel setDisease(String disease) {
        this.disease = disease;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public DiseasePanel setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<String> getGenes() {
        return genes;
    }

    public DiseasePanel setGenes(List<String> genes) {
        this.genes = genes;
        return this;
    }

    public List<String> getRegions() {
        return regions;
    }

    public DiseasePanel setRegions(List<String> regions) {
        this.regions = regions;
        return this;
    }

    public List<String> getVariants() {
        return variants;
    }

    public DiseasePanel setVariants(List<String> variants) {
        this.variants = variants;
        return this;
    }

    public PanelStatus getStatus() {
        return status;
    }

    public DiseasePanel setStatus(PanelStatus status) {
        this.status = status;
        return this;
    }

    public List<DiseasePanelAcl> getAcls() {
        return acls;
    }

    public DiseasePanel setAcls(List<DiseasePanelAcl> acls) {
        this.acls = acls;
        return this;
    }


    public static class PanelStatus extends Status {

        public static final String ARCHIVED = "ARCHIVED";

        public PanelStatus(String status, String message) {
            if (isValid(status)) {
                init(status, message);
            } else {
                throw new IllegalArgumentException("Unknown status " + status);
            }
        }

        public PanelStatus(String status) {
            this(status, "");
        }

        public PanelStatus() {
            this(READY, "");
        }

        public static boolean isValid(String status) {
            if (Status.isValid(status)) {
                return true;
            }
            if (status != null && (status.equals(ARCHIVED))) {
                return true;
            }
            return false;
        }
    }

}

package org.opencb.opencga.core.models.clinical.pharmacogenomics;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opencb.biodata.models.pharma.PharmaChemical;

import java.util.List;

/**
 * Annotation from CellBase pharmacogenomics data for a star allele.
 */
public class StarAlleleAnnotation {

    @JsonProperty("source")
    private String source;

    @JsonProperty("version")
    private String version;

    @JsonProperty("date")
    private String date;

    @JsonProperty("drugs")
    private List<PharmaChemical> drugs;

    public StarAlleleAnnotation() {
    }

    public StarAlleleAnnotation(String source, String version, String date, List<PharmaChemical> drugs) {
        this.source = source;
        this.version = version;
        this.date = date;
        this.drugs = drugs;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<PharmaChemical> getDrugs() {
        return drugs;
    }

    public void setDrugs(List<PharmaChemical> drugs) {
        this.drugs = drugs;
    }
}

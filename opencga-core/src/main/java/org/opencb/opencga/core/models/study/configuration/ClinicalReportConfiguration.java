package org.opencb.opencga.core.models.study.configuration;

import org.opencb.commons.annotations.DataField;

import java.util.Collections;
import java.util.Map;

public class ClinicalReportConfiguration {

    @DataField(id = "title", description = "Title of the clinical report")
    private String title;

    @DataField(id = "logo", description = "Base64 logo image for the clinical report")
    private String logo;

    @DataField(id = "library", description = "Map of library identifiers to their corresponding names or descriptions")
    private Map<String, String> library;

    public ClinicalReportConfiguration() {
    }

    public ClinicalReportConfiguration(String title, String logo, Map<String, String> library) {
        this.title = title;
        this.logo = logo;
        this.library = library;
    }

    public static ClinicalReportConfiguration defaultClinicalReportConfiguration() {
        return new ClinicalReportConfiguration()
                .setTitle("Clinical Report")
                .setLogo("")
                .setLibrary(Collections.emptyMap());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalReportConfiguration{");
        sb.append("title='").append(title).append('\'');
        sb.append(", logo='").append(logo).append('\'');
        sb.append(", library=").append(library);
        sb.append('}');
        return sb.toString();
    }

    public String getTitle() {
        return title;
    }

    public ClinicalReportConfiguration setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getLogo() {
        return logo;
    }

    public ClinicalReportConfiguration setLogo(String logo) {
        this.logo = logo;
        return this;
    }

    public Map<String, String> getLibrary() {
        return library;
    }

    public ClinicalReportConfiguration setLibrary(Map<String, String> library) {
        this.library = library;
        return this;
    }
}

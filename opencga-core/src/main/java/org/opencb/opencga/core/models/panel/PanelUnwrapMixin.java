package org.opencb.opencga.core.models.panel;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;

public interface PanelUnwrapMixin {

    @JsonUnwrapped
    DiseasePanel getDiseasePanel();

}

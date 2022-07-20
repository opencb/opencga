package org.opencb.opencga.core.models.study;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Added for version 2.2.0 to totally deprecate the old `variableSet` field in Variable and rename it to `variables`.
// Delete after version 3.0.0
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

@Deprecated
@JsonIgnoreProperties({"variableSet"})
public interface VariableMixin {
}


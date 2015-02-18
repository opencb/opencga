package org.opencb.opencga.storage.core.variant.io.json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by jacobo on 2/02/15.
 */

@JsonIgnoreProperties({"proteinSubstitutionScores"})
public abstract class VariantAnnotationMixin {
}
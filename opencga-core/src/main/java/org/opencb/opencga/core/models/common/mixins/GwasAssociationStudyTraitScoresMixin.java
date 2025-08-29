package org.opencb.opencga.core.models.common.mixins;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class GwasAssociationStudyTraitScoresMixin {

    @JsonProperty("pValue")
    @JsonAlias("pvalue")
    public abstract Double getPValue();

    @JsonProperty("pValueMlog")
    @JsonAlias("pvalueMlog")
    public abstract Double getPValueMlog();

    @JsonProperty("pValueText")
    @JsonAlias("pvalueText")
    public abstract String getPValueText();

    @JsonProperty("orBeta")
    public abstract Double getOrBeta();

    @JsonProperty("percentCI")
    public abstract String getPercentCI();

}

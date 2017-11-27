package org.opencb.opencga.storage.core.clinical;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opencb.commons.datastore.core.QueryParam.Type.STRING;

public final class ClinicalInterpretationQueryParam implements QueryParam {

    private static final List<ClinicalInterpretationQueryParam> VALUES = new ArrayList<>();

    public static final String CLINICAL_ANALYSIS_ID_DESCR
            = "Clinical analysis ID";
    public static final ClinicalInterpretationQueryParam CLINICAL_ANALYSIS_ID
            = new ClinicalInterpretationQueryParam("caId", STRING, CLINICAL_ANALYSIS_ID_DESCR);

    public static final String FAMILY_NAME_DESCR
            = "Family name";
    public static final ClinicalInterpretationQueryParam FAMIY_NAME
            = new ClinicalInterpretationQueryParam("familyName", STRING, FAMILY_NAME_DESCR);

    public static final String SUBJECT_NAME_DESCR
            = "Subject name";
    public static final ClinicalInterpretationQueryParam SUBJECT_NAME
            = new ClinicalInterpretationQueryParam("subjectName", STRING, SUBJECT_NAME_DESCR);

    private ClinicalInterpretationQueryParam(String key, Type type, String description) {
        this.key = key;
        this.type = type;
        this.description = description;
        VALUES.add(this);
    }

    private final String key;
    private final Type type;
    private final String description;

    @Override
    public String key() {
        return key;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return key() + " [" + type() + "] : " + description();
    }

    public static List<ClinicalInterpretationQueryParam> values() {
        return Collections.unmodifiableList(VALUES);
    }
}

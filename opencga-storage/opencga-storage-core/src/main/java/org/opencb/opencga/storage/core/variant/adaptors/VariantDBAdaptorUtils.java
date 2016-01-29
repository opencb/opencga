package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created on 29/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantDBAdaptorUtils {

    private VariantDBAdaptor adaptor;

    public VariantDBAdaptorUtils(VariantDBAdaptor variantDBAdaptor) {
        adaptor = variantDBAdaptor;
    }

    public StudyConfigurationManager getStudyConfigurationManager() {
        return adaptor.getStudyConfigurationManager();
    }

    public List<Integer> getStudyIds(List studiesNames, QueryOptions options) {
        List<Integer> studiesIds;
        if (studiesNames == null) {
            return Collections.emptyList();
        }
        studiesIds = new ArrayList<>(studiesNames.size());
        for (Object studyObj : studiesNames) {
            Integer studyId = getStudyId(studyObj, options);
            if (studyId != null) {
                studiesIds.add(studyId);
            }
        }
        return studiesIds;
    }

    public Integer getStudyId(Object studyObj, QueryOptions options) {
        return getStudyId(studyObj, options, true);
    }

    public Integer getStudyId(Object studyObj, QueryOptions options, boolean skipNull) {
        Integer studyId;
        if (studyObj instanceof Integer) {
            studyId = ((Integer) studyObj);
        } else {
            String studyName = studyObj.toString();
            if (skipNull && studyName.startsWith("!")) { //Skip negated studies
                studyId = null;
            } else {
                if (StringUtils.isNumeric(studyName)) {
                    studyId = Integer.parseInt(studyName);
                } else {
                    QueryResult<StudyConfiguration> result = getStudyConfigurationManager()
                            .getStudyConfiguration(studyName, options);
                    if (result.getResult().isEmpty()) {
                        throw new IllegalStateException("Study " + studyName + " not found");
                    }
                    studyId = result.first().getStudyId();
                }
            }
        }
        return studyId;
    }

    public int getSampleId(Object sampleObj, StudyConfiguration defaultStudyConfiguration) {
        int sampleId;
        if (sampleObj instanceof Number) {
            sampleId = ((Number) sampleObj).intValue();
        } else {
            String sampleStr = sampleObj.toString();
            if (StringUtils.isNumeric(sampleStr)) {
                sampleId = Integer.parseInt(sampleStr);
            } else {
                if (defaultStudyConfiguration != null) {
                    if (!defaultStudyConfiguration.getSampleIds().containsKey(sampleStr)) {
                        throw new IllegalArgumentException("Sample " + sampleStr + " not found in study "
                                + defaultStudyConfiguration.getStudyName());
                    }
                    sampleId = defaultStudyConfiguration.getSampleIds().get(sampleStr);
                } else {
                    //Unable to identify that sample!
                    List<String> studyNames = getStudyConfigurationManager().getStudyNames(null);
                    throw new IllegalArgumentException("Unknown sample \"" + sampleStr + "\". Please, specify the study belonging."
                            + (studyNames == null ? "" : " Available studies: " + studyNames));
                }
            }
        }
        return sampleId;
    }

}

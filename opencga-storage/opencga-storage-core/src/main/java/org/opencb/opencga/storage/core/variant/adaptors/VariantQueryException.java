package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor.VariantQueryParams;

import java.util.Collections;
import java.util.List;

/**
 * Created on 29/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryException extends IllegalArgumentException {

    public VariantQueryException(String message) {
        super(message);
    }

    public VariantQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public static VariantQueryException malformedParam(VariantQueryParams queryParam, String value) {
        return malformedParam(queryParam, value, "Expected: " + queryParam.description());
    }

    public static VariantQueryException malformedParam(VariantQueryParams queryParam, String value, String message) {
        return new VariantQueryException("Malformed \"" + queryParam.key() + "\" query : \"" + value + "\". "
                +  message);
    }

    public static VariantQueryException studyNotFound(String study) {
        return studyNotFound(study, Collections.emptyList());
    }

    public static VariantQueryException studyNotFound(String study, List<String> availableStudies) {
        return new VariantQueryException("Study { name: \"" + study + "\" } not found."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

    public static VariantQueryException studyNotFound(int studyId) {
        return studyNotFound(studyId, Collections.emptyList());
    }

    public static VariantQueryException studyNotFound(int studyId, List<String> availableStudies) {
        return new VariantQueryException("Study { id: " + studyId + " } not found."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

    public static VariantQueryException missingStudyForSample(String sample, List<String> availableStudies) {
        return new VariantQueryException("Unknown sample \"" + sample + "\". Please, specify the study belonging."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

//    public static VariantQueryException missingStudy() {
//
//    }

    public static VariantQueryException sampleNotFound(Object sample, Object study) {
        return new VariantQueryException("Sample " + sample + " not found in study " + study);
    }

}


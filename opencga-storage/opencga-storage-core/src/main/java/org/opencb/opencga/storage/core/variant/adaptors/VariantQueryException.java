/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.adaptors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryParam;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.GENE;

/**
 * Created on 29/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantQueryException extends IllegalArgumentException {

    private Query query = null;

    public VariantQueryException(String message) {
        super(message);
    }

    public VariantQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public Query getQuery() {
        return query;
    }

    public VariantQueryException setQuery(Query query) {
        if (this.query != null) {
            throw new UnsupportedOperationException();
        }
        this.query = query;
        return this;
    }

    public static VariantQueryException malformedParam(VariantQueryParam queryParam, String value) {
        return malformedParam(queryParam, value, "Expected: " + queryParam.description());
    }

    public static VariantQueryException malformedParam(VariantQueryParam queryParam, String value, String message) {
        return new VariantQueryException("Malformed \"" + queryParam.key() + "\" query : \"" + value + "\". "
                +  message);
    }
    public static VariantQueryException geneNotFound(String gene) {
        return VariantQueryException.malformedParam(GENE, gene, "Gene not found!");
    }

    public static VariantQueryException studyNotFound(String study) {
        return studyNotFound(study, Collections.emptyList());
    }

    public static VariantQueryException studyNotFound(String study, Collection<String> availableStudies) {
        return new VariantQueryException("Study { name: \"" + study + "\" } not found."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

    public static VariantQueryException studyNotFound(int studyId) {
        return studyNotFound(studyId, Collections.emptyList());
    }

    public static VariantQueryException studyNotFound(int studyId, Collection<String> availableStudies) {
        return new VariantQueryException("Study { id: " + studyId + " } not found."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

    public static VariantQueryException cohortNotFound(int cohortId, int studyId, Collection<String> availableCohorts) {
        return cohortNotFound(cohortId, null, studyId, availableCohorts);
    }

    public static VariantQueryException cohortNotFound(String cohortName, int studyId, Collection<String> availableCohorts) {
        return cohortNotFound(null, String.valueOf(cohortName), studyId, availableCohorts);
    }

    private static VariantQueryException cohortNotFound(Number cohortId, String cohortName, int studyId,
                                                        Collection<String> availableCohorts) {
        List<String> availableCohortsList = availableCohorts == null ? Collections.emptyList() : new ArrayList<>(availableCohorts);
        availableCohortsList.sort(String::compareTo);
        return new VariantQueryException("Cohort { "
                + (cohortId != null ? "id: " + cohortId + ' ' : "")
                + (cohortName != null && cohortId != null ? ", " : "")
                + (cohortName != null ? "name: \"" + cohortName + "\" " : "")
                + "} not found in study { id: " + studyId + " }."
                + (availableCohortsList.isEmpty() ? "" : " Available cohorts: " + availableCohortsList));
    }

    public static VariantQueryException missingStudyForSample(String sample, Collection<String> availableStudies) {
        return new VariantQueryException("Unknown sample \"" + sample + "\". Please, specify the study belonging."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

    public static VariantQueryException missingStudyForFile(String file, Collection<String> availableStudies) {
        return new VariantQueryException("Unknown file \"" + file + "\". Please, specify the study belonging."
                + (availableStudies == null || availableStudies.isEmpty() ? "" : " Available studies: " + availableStudies));
    }

//    public static VariantQueryException missingStudy() {
//
//    }

    public static VariantQueryException sampleNotFound(Object sample, Object study) {
        return new VariantQueryException("Sample " + sample + " not found in study " + study);
    }

    public static VariantQueryException fileNotFound(Object file, Object study) {
        return new VariantQueryException("File " + file + " not found in study " + study);
    }

    public static VariantQueryException unknownVariantField(String projectionOp, String field) {
        return new VariantQueryException("Found unknown variant field '" + field + "' in " + projectionOp.toLowerCase());
    }

    public static VariantQueryException unknownVariantAnnotationField(String projectionOp, String field) {
        return new VariantQueryException("Found unknown variant annotation field '" + field + "' in " + projectionOp.toLowerCase());
    }

    public static VariantQueryException internalException(Exception e) {
        return new VariantQueryException("Internal exception: " + e.getMessage(), e);
    }

    public static VariantQueryException unsupportedVariantQueryFilter(QueryParam param, String storageEngineId) {
        return unsupportedVariantQueryFilter(param, storageEngineId, null);
    }

    public static VariantQueryException unsupportedVariantQueryFilter(QueryParam param, String storageEngineId, String extra) {
        return new VariantQueryException("Unsupported variant query filter '" + param.key()
                + "' with storage engine '" + storageEngineId + "'."
                + (StringUtils.isEmpty(extra) ? "" : (' ' + extra)));
    }

    public static VariantQueryException unsupportedVariantQueryFilters(Collection<? extends QueryParam> params, String extra) {
        return new VariantQueryException("Unsupported variant query filters '" + params
                .stream()
                .map(QueryParam::key)
                .collect(Collectors.toList()) + '.'
                + (StringUtils.isEmpty(extra) ? "" : (' ' + extra)));
    }
}


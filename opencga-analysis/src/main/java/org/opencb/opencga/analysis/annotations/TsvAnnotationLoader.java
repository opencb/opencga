/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.annotations;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.analysis.tools.OpenCgaAnalysisTool;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.utils.AnnotationUtils;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.BufferedReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class TsvAnnotationLoader extends OpenCgaAnalysisTool {
    // Variables that need to be passed
    protected String path;
    protected String variableSetId;
    protected String annotationSetId;
    protected String study;

    // Calculated variables during check
    protected Path filePath;

    public void setPath(String path) {
        this.path = path;
    }

    public void setVariableSetId(String variableSetId) {
        this.variableSetId = variableSetId;
    }

    public void setAnnotationSetId(String annotationSetId) {
        this.annotationSetId = annotationSetId;
    }

    public void setStudy(String study) {
        this.study = study;
    }

    @Override
    protected void check() throws Exception {
        String userId = catalogManager.getUserManager().getUserId(token);

        OpenCGAResult<File> fileResult = catalogManager.getFileManager().get(study, path, FileManager.INCLUDE_FILE_URI_PATH, token);
        if (fileResult.getNumResults() == 0) {
            throw new ToolException("File '" + path + "' not found");
        }
        filePath = Paths.get(fileResult.first().getUri());

        OpenCGAResult<VariableSet> variableSetResult = catalogManager.getStudyManager().getVariableSet(study, variableSetId,
                QueryOptions.empty(), token);
        if (variableSetResult.getNumResults() == 0) {
            throw new ToolException("Variable set '" + variableSetId + "' not found");
        }

        try (BufferedReader bufferedReader = FileUtils.newBufferedReader(filePath)) {
            List<String> columns = getFields(bufferedReader.readLine());
            // First column should contain any ID; the variables will correspond to the rest of columns.
            for (int i = 1; i < columns.size(); i++) {
                AnnotationUtils.checkVariableIdInVariableSet(columns.get(i), variableSetResult.first().getVariables());
            }
            List<String> sampleIds = new LinkedList<>();
            boolean eof = false;
            while (!eof) {
                List<String> values = getFields(bufferedReader.readLine());
                if (values.isEmpty()) {
                    eof = true;
                } else {
                    // Add the sample id
                    sampleIds.add(values.get(0));
                }
            }

            // Check user has corresponding permissions to update the list of samples.
            Query query = new Query()
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds)
                    .append(ParamConstants.ACL_PARAM, userId + ":" + StringUtils.join(Arrays.asList(SampleAclEntry.SamplePermissions.VIEW,
                            SampleAclEntry.SamplePermissions.WRITE, SampleAclEntry.SamplePermissions.WRITE_ANNOTATIONS), ","));
            int numResults = count(query);
            if (numResults < sampleIds.size()) {
                throw new ToolException("Cannot create annotations for all the entries. Missing permissions in "
                        + (sampleIds.size() - numResults) + " out of " + sampleIds.size() + " entries.");
            }

            // Check user does not have any annotations using the annotation set id.
            query = new Query()
                    .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds)
                    .append(Constants.ANNOTATION, Constants.ANNOTATION_SET_NAME + "=" + annotationSetId);
            numResults = count(query);
            if (numResults > 0) {
                throw new ToolException("Found " + numResults + " entries with AnnotationSet id '" + annotationSetId + "'");
            }
        }
    }

    /**
     * Split the line by the tab symbol.
     *
     * @param lineContent Content of a line read.
     * @return Splitted list of values.
     */
    private List<String> getFields(String lineContent) {
        return lineContent != null ? Arrays.asList(StringUtils.split(lineContent, "\t")) : Collections.emptyList();
    }

    @Override
    protected void run() throws Exception {
        try (BufferedReader bufferedReader = FileUtils.newBufferedReader(filePath)) {
            List<String> columns = getFields(bufferedReader.readLine());

            boolean eof = false;
            while (!eof) {
                List<String> values = getFields(bufferedReader.readLine());
                if (values.isEmpty()) {
                    eof = true;
                } else {
                    String sampleId = values.get(0);

                    // Generate annotation map
                    ObjectMap annotations = new ObjectMap();
                    for (int i = 1; i < values.size(); i++) {
                        annotations.put(columns.get(i), values.get(i), true, true);
                    }
                    // Add annotationset
                    AnnotationSet annotationSet = new AnnotationSet(variableSetId, variableSetId, annotations);
                    addAnnotationSet(sampleId, annotationSet, QueryOptions.empty());
                }
            }
        }
    }

    public abstract int count(Query query) throws CatalogException;

    public abstract void addAnnotationSet(String entryId, AnnotationSet annotationSet, QueryOptions options) throws CatalogException;
}

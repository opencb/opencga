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

package org.opencb.opencga.storage.core.clinical;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

public interface ClinicalVariantEngine {

    enum QueryParams implements QueryParam {
        CLINICAL_ANALYSIS_ID("clinicalAnalysisId", INTEGER, ""),
        FAMILY("family", TEXT_ARRAY, ""),
        SUBJECT("subject", TEXT_ARRAY, ""),
        SAMPLE("sample", TEXT_ARRAY, ""),

        STUDY_ID("studyId", INTEGER_ARRAY, ""),
        STUDY("study", INTEGER_ARRAY, ""); // Alias to studyId in the database. Only for the webservices.

        private static Map<String, QueryParams> map;
        static {
            map = new LinkedHashMap<>();
            for (QueryParams params : QueryParams.values()) {
                map.put(params.key(), params);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    void create(String dbName) throws ClinicalVariantException;

    boolean isAlive(String collection) throws ClinicalVariantException;

    boolean exists(String dbName) throws ClinicalVariantException;

    void insert(Interpretation interpretation, String collection) throws IOException, ClinicalVariantException;

    /**
     * Insert a list of Interpretation objects into Solr: previously each Interpretation object is
     * converted to multiple ClinicalVariantSearchModel objects and they will be stored in Solr.
     *
     * @param interpretations   List of Interpretation objects to insert
     * @param collection        Solr collection where to insert
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    default void insert(List<Interpretation> interpretations, String collection) throws IOException, ClinicalVariantException {
        if (ListUtils.isNotEmpty(interpretations)) {
            for (Interpretation interpretation: interpretations) {
                insert(interpretation, collection);
            }
        }
    }

    /**
     * Load a JSON file containing Interpretation object into the Solr core/collection.
     *
     * @param interpretationJsonPath          Path to the JSON file containing the Interpretation objects
     * @param collection    Solr collection where to insert
     * @throws IOException                      IOException
     * @throws ClinicalVariantException   ClinicalVariantException
     */
    default void insert(Path interpretationJsonPath, String collection) throws IOException, ClinicalVariantException {
        FileUtils.checkFile(interpretationJsonPath);

        ObjectReader objectReader = new ObjectMapper().readerFor(Interpretation.class);
        Interpretation interpretation = objectReader.readValue(interpretationJsonPath.toFile());
        insert(interpretation, collection);
    }

    DataResult<ClinicalVariant> query(Query query, QueryOptions options, String collection)
            throws IOException, ClinicalVariantException;

    DataResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String collection)
                    throws IOException, ClinicalVariantException;

    DataResult<FacetField> facet(Query query, QueryOptions queryOptions, String collection)
            throws IOException, ClinicalVariantException;

    ClinicalVariantIterator iterator(Query query, QueryOptions options, String collection)
            throws ClinicalVariantException, IOException;


    void addInterpretationComment(long interpretationId, Comment comment, String collection)
                                    throws IOException, ClinicalVariantException;

    void addClinicalVariantComment(long interpretationId, String variantId, Comment comment, String collection)
                                            throws IOException, ClinicalVariantException;

    void setStorageConfiguration(StorageConfiguration storageConfiguration);
}

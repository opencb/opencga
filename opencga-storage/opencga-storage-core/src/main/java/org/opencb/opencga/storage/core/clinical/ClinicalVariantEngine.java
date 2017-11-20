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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.clinical.Comment;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedVariant;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface ClinicalVariantEngine {

    void create(String dbName) throws ClinicalVariantException;

    boolean isAlive(String collection) throws ClinicalVariantException;

    boolean exists(String dbName) throws ClinicalVariantException;

    void insert(Interpretation interpretation, String collection) throws IOException, ClinicalVariantException;

    /**
     * Insert a list of Interpretation objects into Solr: previously each Interpretation object is
     * converted to multiple ReportedVariantSearchModel objects and they will be stored in Solr.
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

    QueryResult<ReportedVariant> query(Query query, QueryOptions options, String collection)
            throws IOException, ClinicalVariantException;

    QueryResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String collection)
                    throws IOException, ClinicalVariantException;

    ReportedVariantIterator iterator(Query query, QueryOptions options, String collection)
            throws ClinicalVariantException, IOException;

    void addInterpretationComment(long interpretationId, Comment comment, String collection)
                                    throws IOException, ClinicalVariantException;

    void addReportedVariantComment(long interpretationId, String variantId, Comment comment, String collection)
                                            throws IOException, ClinicalVariantException;
}

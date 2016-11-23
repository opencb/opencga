/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.storage.core.local;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class VariantStorageManager extends StorageManager {

    private org.opencb.opencga.storage.core.variant.VariantStorageManager variantStorageManager;


    public VariantStorageManager() {
    }

    public VariantStorageManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) {
        super(catalogManager, storageConfiguration);


        this.logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    public void clearCache(String studyId, String type, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

    }


    public void importData(String fileId, String studyId, String sessionId) {

    }

    public void exportData(String outputFile, String studyId, String sessionId) {

    }
    public void exportData(String outputFile, String studyId, Query query, QueryOptions queryOptions, String sessionId) {

    }



    public void index(String fileId, String studyId, ObjectMap config, String sessionId) {

    }

    public void index(List<String> fileId, String studyId, ObjectMap config, String sessionId) {

    }

    public void deleteStudy(String studyId, String sessionId) {

    }

    public void deleteFile(String fileId, String studyId, String sessionId) {

    }

    public void addAnnotation(String annotationId, String studyId, Query query, String sessionId) {

    }

    public void deleteAnnotation(String annotationId, String studyId, String sessionId) {

    }

    public void stats(String studyId, List<String> cohorts, Query query, String sessionId) {

    }

    public void deleteStats(List<String> cohorts, String studyId, String sessionId) {

    }




    public QueryResult<Variant> get(Query query, QueryOptions queryOptions, String sessionId) {
        return null;
    }

//    public QueryResult groupBy(GroupByFieldEnum field, Query query, QueryOptions queryOptions, String sessionId) {
//        return null;
//    }

    public QueryResult rank(Query query, String field, int limt, boolean asc, String sessionId) throws StorageManagerException {
        try {
            Long id = catalogManager.getStudyManager().getId("", "");
            Study first = catalogManager.getStudyManager().get(id, null, sessionId).first();
            String variant = first.getDataStores().get("VARIANT").getDbName();
            QueryResult rank = variantStorageManager.getDBAdaptor(variant).rank(query, field, limt, asc);


        } catch (CatalogException e) {
            e.printStackTrace();
        }
        return null;
    }

    public QueryResult<Long> count(Query query, String sessionId) {
        return null;
    }

    public QueryResult<String> distinct(Query query, String field, String sessionId) {
        return null;
    }

    public void facet() {

    }

    public void getPhased(Variant variant, String studyId, String sampleId, String sessionId) {

    }

    public void getFrequency() {

    }

    public VariantDBIterator iterator(String sessionIdi) {
        return null;
    }

    public VariantDBIterator iterator(Query query, QueryOptions queryOptions, String sessionId) {
        return null;
    }

//    public <T> VariantDBIterator<T> iterator(Query query, QueryOptions queryOptions, Class<T> clazz, String sessionId) {
//        return null;
//    }

    public void intersect(Query query, QueryOptions queryOptions, List<String> studyId1, String sessionId) {

    }


    Map<Long, List<Sample>> getSamplesMetadata(Query query, QueryOptions queryOptions, String sessionId)  {
        return null;
    }


    @Override
    public void testConnection() throws StorageManagerException {

    }
}

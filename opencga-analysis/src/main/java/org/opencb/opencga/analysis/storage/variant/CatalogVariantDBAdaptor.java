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

package org.opencb.opencga.analysis.storage.variant;


import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import java.util.*;

/**
* Created by hpccoll1 on 13/02/15.
*/
@Deprecated
public abstract class CatalogVariantDBAdaptor implements VariantDBAdaptor {

    private final CatalogManager catalogManager;
    private final VariantDBAdaptor dbAdaptor;

    public CatalogVariantDBAdaptor(CatalogManager catalogManager, VariantDBAdaptor dbAdaptor) {
        this.catalogManager = catalogManager;
        this.dbAdaptor = dbAdaptor;
    }

    public CatalogVariantDBAdaptor(CatalogManager catalogManager, String fileId, String sessionId) throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException, StorageEngineException {
        this.catalogManager = catalogManager;
        this.dbAdaptor = buildDBAdaptor(catalogManager, fileId, sessionId);
    }

    private static VariantDBAdaptor buildDBAdaptor(CatalogManager catalogManager, String fileId, String sessionId) throws CatalogException, ClassNotFoundException, IllegalAccessException, InstantiationException, StorageEngineException {
        long id = catalogManager.getFileId(fileId);
        File file = catalogManager.getFile(id, sessionId).getResult().get(0);
        String dbName = file.getAttributes().get("dbName").toString();
        String storageEngine = file.getAttributes().get("storageEngine").toString();
        return StorageEngineFactory.get().getVariantStorageEngine(storageEngine).getDBAdaptor(dbName);
    }

    @Override
    public void setDataWriter(DataWriter dataWriter) {
        dbAdaptor.setDataWriter(dataWriter);
    }

//    @Override
//    public QueryResult<Variant> getAllVariants(QueryOptions options) {
//
//        try {
//            checkQueryOptions(options);
//        } catch (Exception e) {
//            return queryError("getAllVariants", e);
//        }
//        return dbAdaptor.getAllVariants(options);
//    }

    private void checkQueryOptions(QueryOptions options) throws CatalogException {
        Map<Integer, Study> studiesMap = getStudiesMap(options);
        Map<Integer, File> filesMap = getFilesMap(options);
        checkFiles(filesMap.values());
    }

//    @Override
//    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
//        try {
//            checkQueryOptions(options);
//        } catch (Exception e) {
//            return queryError("getAllVariants", e);
//        }
//        return dbAdaptor.getVariantById(id, options);
//    }
//
//    @Override
//    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
//        try {
//            checkQueryOptions(options);
//        } catch (Exception e) {
//            return Collections.singletonList(this.<Variant>queryError("getAllVariants", e));
//        }
//        return dbAdaptor.getAllVariantsByIdList(idList, options);
//    }
//
//    @Override
//    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
//        try {
//            checkQueryOptions(options);
//        } catch (Exception e) {
//            return queryError("getAllVariants", e);
//        }
//        return dbAdaptor.getAllVariantsByRegion(region, options);
//    }
//
//    @Override
//    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
//        try {
//            checkQueryOptions(options);
//        } catch (Exception e) {
//            e.printStackTrace();
//            return Collections.singletonList(this.<Variant>queryError("getAllVariants", e));
//        }
//        return dbAdaptor.getAllVariantsByRegionList(regionList, options);
//    }
//
//    @Override
//    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
//        try {
//            checkQueryOptions(options);
//        } catch (Exception e) {
//            return this.<Variant>queryError("getAllVariants", e);
//        }
//        return dbAdaptor.getVariantFrequencyByRegion(region, options);
//    }
//
//    @Override
//    public QueryResult groupBy(String field, QueryOptions options) {
//        return null;
//    }
//
//    @Override
//    public QueryResult getAllVariantsByGene(String geneName, QueryOptions options) {
//        return null;
//    }
//
//    @Deprecated
//    @Override
//    public QueryResult getMostAffectedGenes(int numGenes, QueryOptions options) {
//        return null;
//    }
//
//
//    @Override
//    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
//        return null;
//    }
//
//    @Override
//    public VariantDBIterator iterator() {
//        return null;
//    }
//
//    @Override
//    public VariantDBIterator iterator(QueryOptions options) {
//        return null;
//    }
//
//    @Override
//    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
//        return null;
//    }
//
//    @Override
//    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions) {
//        return null;
//    }
//
//    @Override
//    public boolean close() {
//        return false;
//    }


    //AuxMethods

    private Map<Integer, File> getFilesMap(QueryOptions options) throws CatalogException {
        String sessionId = options.getString("sessionId");
        Object files = options.get(VariantQueryParams.FILES.key());
        List<Integer> fileIds = getIntegerList(files);
        return getFilesMap(fileIds, sessionId);
    }

    private Map<Integer, Study> getStudiesMap(QueryOptions options) throws CatalogException {
        String sessionId = options.getString("sessionId");
        Object files = options.get(VariantQueryParams.STUDIES.key());
        List<Integer> fileIds = getIntegerList(files);
        return getStudiesMap(fileIds, sessionId);
    }

    private List<Integer> getIntegerList(Object objedt) {
        List<Integer> list = new LinkedList<>();
        if (objedt == null) {
            return Collections.emptyList();
        } else if (objedt instanceof List) {
            for (Object o : ((List) objedt)) {
                list.add(Integer.parseInt("" + o));
            }
        } else {
            for (String s : objedt.toString().split(",")) {
                list.add(Integer.parseInt(s));
            }
        }
        return list;
    }

    private Map<Integer, File> getFilesMap(List<Integer> files, String sessionId) throws CatalogException {
        Map<Integer, File> fileMap;
        fileMap = new HashMap<>();
        for (Integer fileId : files) {
            QueryResult<File> fileQueryResult = catalogManager.getFile(fileId, sessionId);
            File file = fileQueryResult.getResult().get(0);
            fileMap.put(fileId, file);
        }
        return fileMap;
    }
    private Map<Integer, Study> getStudiesMap(List<Integer> studies, String sessionId) throws CatalogException {
        Map<Integer, Study> studyMap;
        QueryOptions options = new QueryOptions("include", Collections.singletonList("projects.studies.id"));
        studyMap = new HashMap<>();
        for (Integer studyId : studies) {
            QueryResult<Study> fileQueryResult = catalogManager.getStudy(studyId, options, sessionId);
            Study s = fileQueryResult.getResult().get(0);
            studyMap.put(studyId, s);
        }
        return studyMap;
    }

    private void checkFiles(Collection<File> values) throws CatalogException {
        for (File file : values) {
            if (!file.getType().equals(File.Type.FILE)) {
                throw new CatalogException("Expected file type = FILE");
            } else if (!file.getBioformat().equals(File.Bioformat.VARIANT)) {
                throw new CatalogException("Expected file bioformat = VARIANT");
            }
        }
    }

    private <T> QueryResult<T> queryError(String id, Exception e) {
        return new QueryResult<T>(id, 0, 0, 0, "", e.getMessage(), Collections.<T>emptyList());
    }

//    private QueryResult queryError(String id, Exception e) {
//        return new QueryResult(id, 0, 0, 0, "", e.getMessage(), Collections.emptyList());
//    }



}

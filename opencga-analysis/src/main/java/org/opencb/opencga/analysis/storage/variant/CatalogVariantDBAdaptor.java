package org.opencb.opencga.analysis.storage.variant;


import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.beans.Study;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.util.*;

/**
* Created by hpccoll1 on 13/02/15.
*/
//public class CatalogVariantDBAdaptor {}
public class CatalogVariantDBAdaptor implements VariantDBAdaptor {

    private final CatalogManager catalogManager;
    private final VariantDBAdaptor dbAdaptor;

    public CatalogVariantDBAdaptor(CatalogManager catalogManager, VariantDBAdaptor dbAdaptor) {
        this.catalogManager = catalogManager;
        this.dbAdaptor = dbAdaptor;
    }

    public CatalogVariantDBAdaptor(CatalogManager catalogManager, String fileId, String sessionId) throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException, StorageManagerException {
        this.catalogManager = catalogManager;
        this.dbAdaptor = buildBAdaptor(catalogManager, fileId, sessionId);
    }

    private static VariantDBAdaptor buildBAdaptor(CatalogManager catalogManager, String fileId, String sessionId) throws CatalogException, ClassNotFoundException, IllegalAccessException, InstantiationException, StorageManagerException {
        int id = catalogManager.getFileId(fileId);
        File file = catalogManager.getFile(id, sessionId).getResult().get(0);
        String dbName = file.getAttributes().get("dbName").toString();
        String storageEngine = file.getAttributes().get("storageEngine").toString();
        return StorageManagerFactory.getVariantStorageManager(storageEngine).getDBAdaptor(dbName, new ObjectMap());
    }

    @Override
    public void setDataWriter(DataWriter dataWriter) {
        dbAdaptor.setDataWriter(dataWriter);
    }

    @Override
    public QueryResult<Variant> getAllVariants(QueryOptions options) {

        try {
            checkQueryOptions(options);
        } catch (Exception e) {
            return queryError("getAllVariants", e);
        }
        return dbAdaptor.getAllVariants(options);
    }

    private void checkQueryOptions(QueryOptions options) throws CatalogException {
        Map<Integer, Study> studiesMap = getStudiesMap(options);
        Map<Integer, File> filesMap = getFilesMap(options);
        checkFiles(filesMap.values());
    }

    @Override
    public QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        try {
            checkQueryOptions(options);
        } catch (Exception e) {
            return queryError("getAllVariants", e);
        }
        return dbAdaptor.getVariantById(id, options);
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        try {
            checkQueryOptions(options);
        } catch (Exception e) {
            return Collections.singletonList(this.<Variant>queryError("getAllVariants", e));
        }
        return dbAdaptor.getAllVariantsByIdList(idList, options);
    }

    @Override
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        try {
            checkQueryOptions(options);
        } catch (Exception e) {
            return queryError("getAllVariants", e);
        }
        return dbAdaptor.getAllVariantsByRegion(region, options);
    }

    @Override
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        try {
            checkQueryOptions(options);
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.singletonList(this.<Variant>queryError("getAllVariants", e));
        }
        return dbAdaptor.getAllVariantsByRegionList(regionList, options);
    }

    @Override
    public QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        try {
            checkQueryOptions(options);
        } catch (Exception e) {
            return this.<Variant>queryError("getAllVariants", e);
        }
        return dbAdaptor.getVariantFrequencyByRegion(region, options);
    }

    @Override
    public QueryResult groupBy(String field, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult getAllVariantsByGene(String geneName, QueryOptions options) {
        return null;
    }

    @Deprecated
    @Override
    public QueryResult getMostAffectedGenes(int numGenes, QueryOptions options) {
        return null;
    }


    @Override
    public VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        return null;
    }

    @Override
    public VariantDBIterator iterator() {
        return null;
    }

    @Override
    public VariantDBIterator iterator(QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, QueryOptions queryOptions) {
        return null;
    }

    @Override
    public boolean close() {
        return false;
    }


    //AuxMethods

    private Map<Integer, File> getFilesMap(QueryOptions options) throws CatalogException {
        String sessionId = options.getString("sessionId");
        Object files = options.get(VariantDBAdaptor.FILES);
        List<Integer> fileIds = getIntegerList(files);
        return getFilesMap(fileIds, sessionId);
    }

    private Map<Integer, Study> getStudiesMap(QueryOptions options) throws CatalogException {
        String sessionId = options.getString("sessionId");
        Object files = options.get(VariantDBAdaptor.STUDIES);
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
            QueryResult<Study> fileQueryResult = catalogManager.getStudy(studyId, sessionId, options);
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


    // DEPRECATED METHODS
    @Deprecated
    @Override
    public QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyIds, QueryOptions options) {
        throw new UnsupportedOperationException("Deprecated method");
    }
    @Deprecated
    @Override
    public QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options) {
        throw new UnsupportedOperationException("Deprecated method");
    }

    @Deprecated
    @Override
    public QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        throw new UnsupportedOperationException("Deprecated method");
    }

    @Deprecated
    @Override
    public QueryResult getBottomConsequenceTypes(int numConsequenceTypes, QueryOptions options) {
        throw new UnsupportedOperationException("Deprecated method");
    }


}

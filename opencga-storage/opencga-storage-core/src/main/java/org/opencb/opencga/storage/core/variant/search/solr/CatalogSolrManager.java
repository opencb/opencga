package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.opencb.commons.datastore.core.ComplexTypeConverter;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.stats.solr.FileSolrModel;
import org.opencb.opencga.catalog.stats.solr.SampleSolrModel;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogFileToSolrFileConverter;
import org.opencb.opencga.catalog.stats.solr.converters.CatalogSampleToSolrSampleConverter;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wasim on 27/06/18.
 */
public class CatalogSolrManager {

    private CatalogManager catalogManager;
    private SolrManager solrManager;
    private StorageConfiguration storageConfiguration;
    private CatalogSampleToSolrSampleConverter catalogSampleToSolrSampleConverter;

    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;

    public static final String COHORT_SOLR_COLLECTION = "Catalog_Cohort_Collection";
    public static final String FILE_SOLR_COLLECTION = "Catalog_FILE_Collection";
    public static final String FAMILY_SOLR_COLLECTION = "Catalog_Family_Collection";
    public static final String INDIVIDUAL_SOLR_COLLECTION = "Catalog_Individual_Collection";
    public static final String SAMPLES_SOLR_COLLECTION = "Catalog_Sample_Collection";

    public static final String COHORT_CONF_SET = "OpenCGACatalogCohortConfSet";
    public static final String FILE_CONF_SET = "OpenCGACatalogFileConfSet";
    public static final String SAMPLE_CONF_SET = "OpenCGACatalogSampleConfSet";
    public static final String FAMILY_CONF_SET = "OpenCGACatalogFamilyConfSet";
    public static final String INDIVIDUAL_CONF_SET = "OpenCGACatalogIndividualConfSet";
    public static final String SEARCH_ENGINE_ID = "solr";
    private int insertBatchSize;


    private Logger logger;

    public CatalogSolrManager(CatalogManager catalogManager, StorageConfiguration storageConfiguration) throws VariantSearchException {
        this.catalogManager = catalogManager;
        this.storageConfiguration = storageConfiguration;
        this.solrManager = new SolrManager(storageConfiguration.getSearch().getHost(), storageConfiguration.getSearch().getMode(),
                storageConfiguration.getSearch().getTimeout());
        this.catalogSampleToSolrSampleConverter = new CatalogSampleToSolrSampleConverter();
        insertBatchSize = DEFAULT_INSERT_BATCH_SIZE;
        if (storageConfiguration.getSearch().getMode().equals("cloud")) {
            createCatalogSolrCollections();
        } else {
            createCatalogSolrCores();
        }

        logger = LoggerFactory.getLogger(VariantSearchManager.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    public void create(String dbName, String configSet) throws VariantSearchException {
        solrManager.create(dbName, configSet);
    }

    public void createCore(String coreName, String configSet) throws VariantSearchException {
        solrManager.createCore(coreName, configSet);
    }

    public void createCollection(String collectionName, String configSet) throws VariantSearchException {
        solrManager.createCollection(collectionName, configSet);
    }

    public boolean exists(String dbName) throws VariantSearchException {
        return solrManager.exists(dbName);
    }

    public boolean existsCore(String coreName) {
        return solrManager.existsCore(coreName);
    }

    public boolean existsCollection(String collectionName) throws VariantSearchException {
        return solrManager.existsCollection(collectionName);
    }

    public void createCatalogSolrCollections() throws VariantSearchException {

        if (!existsCollection(COHORT_SOLR_COLLECTION)) {
            createCollection(COHORT_SOLR_COLLECTION, COHORT_CONF_SET);
        }
        if (!existsCollection(FAMILY_SOLR_COLLECTION)) {
            createCollection(FAMILY_SOLR_COLLECTION, FAMILY_CONF_SET);
        }
        if (!existsCollection(FILE_SOLR_COLLECTION)) {
            createCollection(FILE_SOLR_COLLECTION, FILE_CONF_SET);
        }
        if (!existsCollection(INDIVIDUAL_SOLR_COLLECTION)) {
            createCollection(INDIVIDUAL_SOLR_COLLECTION, INDIVIDUAL_CONF_SET);
        }
        if (!existsCollection(SAMPLES_SOLR_COLLECTION)) {
            createCollection(SAMPLES_SOLR_COLLECTION, SAMPLE_CONF_SET);
        }
    }

    public void createCatalogSolrCores() throws VariantSearchException {

        if (!existsCore(COHORT_SOLR_COLLECTION)) {
            createCore(COHORT_SOLR_COLLECTION, COHORT_CONF_SET);
        }
        if (!existsCore(FAMILY_SOLR_COLLECTION)) {
            createCore(FAMILY_SOLR_COLLECTION, FAMILY_CONF_SET);
        }
        if (!existsCore(FILE_SOLR_COLLECTION)) {
            createCore(FILE_SOLR_COLLECTION, FILE_CONF_SET);
        }
        if (!existsCore(INDIVIDUAL_SOLR_COLLECTION)) {
            createCore(INDIVIDUAL_SOLR_COLLECTION, INDIVIDUAL_CONF_SET);
        }
        if (!existsCore(SAMPLES_SOLR_COLLECTION)) {
            createCore(SAMPLES_SOLR_COLLECTION, SAMPLE_CONF_SET);
        }
    }

    public void indexCatalogSamples(Query query) throws CatalogException, SolrServerException, IOException, VariantSearchException {

        DBIterator<Sample> iterator = catalogManager.getSampleManager().indexSolr(query);

        int count = 0;
        List<Sample> sampleList = new ArrayList<>(insertBatchSize);
        while (iterator.hasNext()) {
            Sample sample = iterator.next();
            sampleList.add(sample);
            count++;
            if (count % insertBatchSize == 0) {
                indexSamples(sampleList);
                sampleList.clear();
            }
        }

        if (CollectionUtils.isNotEmpty(sampleList)) {
            indexSamples(sampleList);
        }
    }


    public void indexCatalogFiles(Query query) throws CatalogException, SolrServerException, IOException, VariantSearchException {

        DBIterator<File> iterator = catalogManager.getFileManager().indexSolr(query);

        int count = 0;
        List<File> fileList = new ArrayList<>(insertBatchSize);
        while (iterator.hasNext()) {
            File file = iterator.next();
            fileList.add(file);
            count++;
            if (count % insertBatchSize == 0) {
                indexFiles(fileList);
                fileList.clear();
            }
        }

        if (CollectionUtils.isNotEmpty(fileList)) {
            indexFiles(fileList);
        }
    }

 /*   public void indexCatalogCohorts(Query query) throws CatalogException, SolrServerException, IOException, VariantSearchException {

        DBIterator<Cohort> iterator = catalogManager.getCohortManager().indexSolr(query);

        int count = 0;
        List<Cohort> cohorts = new ArrayList<>(insertBatchSize);
        while (iterator.hasNext()) {
            Cohort cohort = iterator.next();
            cohorts.add(cohort);
            count++;
            if (count % insertBatchSize == 0) {
                indexCohorts(cohorts);
                cohorts.clear();
            }
        }

        if (CollectionUtils.isNotEmpty(cohorts)) {
            indexCohorts(cohorts);
        }
    }*/

    public void indexSamples(List<Sample> samples) throws IOException, SolrServerException, VariantSearchException {
        CatalogSampleToSolrSampleConverter sampleToSolrSampleConverter = new CatalogSampleToSolrSampleConverter();
        List<SampleSolrModel> sampleSolrModels = new ArrayList<>();

        for (Sample sample : samples) {
            sampleSolrModels.add(sampleToSolrSampleConverter.convertToStorageType(sample));
        }

        UpdateResponse updateResponse;
        try {
            updateResponse = solrManager.getSolrClient().addBeans(SAMPLES_SOLR_COLLECTION, sampleSolrModels);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(SAMPLES_SOLR_COLLECTION);
            }
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    public void indexFiles(List<File> files) throws IOException, SolrServerException, VariantSearchException {
        CatalogFileToSolrFileConverter fileToSolrFileConverter = new CatalogFileToSolrFileConverter();
        List<FileSolrModel> fileSolrModels = new ArrayList<>();

        for (File file : files) {
            fileSolrModels.add(fileToSolrFileConverter.convertToStorageType(file));
        }

        UpdateResponse updateResponse;
        try {
            updateResponse = solrManager.getSolrClient().addBeans(FILE_SOLR_COLLECTION, fileSolrModels);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(FILE_SOLR_COLLECTION);
            }
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }

    public <T, M> void indexCatalogCollection(List<T> records, ComplexTypeConverter converter, M solrModelType,
                                              String collectionName) throws IOException, SolrServerException, VariantSearchException {
        List<M> solrModels = new ArrayList<>();

        for (T record : records) {
            solrModels.add((M) converter.convertToStorageType(record));
        }

        UpdateResponse updateResponse;
        try {
            updateResponse = solrManager.getSolrClient().addBeans(collectionName, solrModels);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(collectionName);
            }
        } catch (SolrServerException e) {
            throw new VariantSearchException(e.getMessage(), e);
        }
    }
}


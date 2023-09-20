package org.opencb.opencga.server;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.Event;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class OpenCGAHealthCheckMonitor {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicReference<HealthCheckStatus> cachedResult = new AtomicReference<>();

    private final Configuration configuration;
    private final CatalogManager catalogManager;
    private final StorageEngineFactory storageEngineFactory;
    private final VariantStorageManager variantManager;

    public OpenCGAHealthCheckMonitor(Configuration configuration, CatalogManager catalogManager,
                                     StorageEngineFactory storageEngineFactory,
                                     VariantStorageManager variantManager) {
        this.configuration = configuration;
        this.catalogManager = catalogManager;
        this.storageEngineFactory = storageEngineFactory;
        this.variantManager = variantManager;
    }

    public static class HealthCheckStatus {

        enum Status {
            OK,
            KO,
            NA
        }

        @JsonProperty("CatalogMongoDB")
        private Status catalogMongoDbStatus = null;
        @JsonProperty("Solr")
        private Status solrStatus = null;
        @JsonProperty("VariantStorage")
        private Status variantStorageStatus = null;

        @JsonProperty("VariantStorageId")
        private String variantStorageId = "";

        @JsonIgnore
        private String errorMessage = null;
        @JsonIgnore
        private LocalDateTime creationDate;
        @JsonIgnore
        private boolean healthy;

        public HealthCheckStatus() {
        }

        public Status getCatalogMongoDbStatus() {
            return catalogMongoDbStatus;
        }

        public HealthCheckStatus setCatalogMongoDbStatus(Status catalogMongoDbStatus) {
            this.catalogMongoDbStatus = catalogMongoDbStatus;
            return this;
        }

        public Status getSolrStatus() {
            return solrStatus;
        }

        public HealthCheckStatus setSolrStatus(Status solrStatus) {
            this.solrStatus = solrStatus;
            return this;
        }

        public Status getVariantStorageStatus() {
            return variantStorageStatus;
        }

        public HealthCheckStatus setVariantStorageStatus(Status variantStorageStatus) {
            this.variantStorageStatus = variantStorageStatus;
            return this;
        }

        public String getVariantStorageId() {
            return variantStorageId;
        }

        public HealthCheckStatus setVariantStorageId(String variantStorageId) {
            this.variantStorageId = variantStorageId;
            return this;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public HealthCheckStatus setHealthy(boolean healthy) {
            this.healthy = healthy;
            return this;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public HealthCheckStatus setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }

        public LocalDateTime getCreationDate() {
            return creationDate;
        }

        public HealthCheckStatus setCreationDate(LocalDateTime creationDate) {
            this.creationDate = creationDate;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("HealthCheckStatus{");
            sb.append("catalogMongoDbStatus='").append(catalogMongoDbStatus).append('\'');
            sb.append(", solrStatus='").append(solrStatus).append('\'');
            sb.append(", variantStorageStatus='").append(variantStorageStatus).append('\'');
            sb.append(", variantStorageId='").append(variantStorageId).append('\'');
            sb.append(", errorMessage='").append(errorMessage).append('\'');
            sb.append(", creationDate=").append(creationDate);
            sb.append(", healthy=").append(healthy);
            sb.append('}');
            return sb.toString();
        }
    }

    public OpenCGAResult<HealthCheckStatus> getStatus() {

        OpenCGAResult<HealthCheckStatus> queryResult = new OpenCGAResult<>();
        StopWatch stopWatch = StopWatch.createStarted();

        if (shouldUpdateStatus()) {
            logger.debug("Update HealthCheck cache status");
            updateHealthCheck();
        } else {
            HealthCheckStatus status = cachedResult.get();
            String msg = "HealthCheck results from cache at " + status.getCreationDate().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            queryResult.setEvents(Collections.singletonList(new Event(Event.Type.INFO, msg)));
            logger.debug(msg);
        }

        queryResult.setTime(((int) stopWatch.getTime(TimeUnit.MILLISECONDS)));
        queryResult.setResults(Collections.singletonList(cachedResult.get()));
        return queryResult;
    }

    private boolean shouldUpdateStatus() {
        HealthCheckStatus status = cachedResult.get();
        if (status == null || !status.isHealthy()) {
            // Always update if not healthy or undefined
            return true;
        }
        // If healthy, only update every "healthCheck.interval" seconds
        long elapsedTime = Duration.between(status.getCreationDate(), LocalDateTime.now()).getSeconds();
        return elapsedTime > configuration.getHealthCheck().getInterval();
    }

    private synchronized void updateHealthCheck() {
        if (!shouldUpdateStatus()) {
            // Skip update!
            return;
        }
        StringBuilder errorMsg = new StringBuilder();
        boolean healthy = true;

        HealthCheckStatus status = new HealthCheckStatus();

        StopWatch totalTime = StopWatch.createStarted();
        StopWatch catalogMongoDBTime = StopWatch.createStarted();
        try {
            if (catalogManager.getCatalogDatabaseStatus()) {
                status.setCatalogMongoDbStatus(HealthCheckStatus.Status.OK);
            } else {
                status.setCatalogMongoDbStatus(HealthCheckStatus.Status.KO);
                healthy = false;
            }
        } catch (Exception e) {
            status.setCatalogMongoDbStatus(HealthCheckStatus.Status.KO);
            healthy = false;
            errorMsg.append(e.getMessage());
            logger.error("Error reading catalog status", e);
        }
        catalogMongoDBTime.stop();

        StopWatch storageTime = StopWatch.createStarted();
        try {
            storageEngineFactory.getVariantStorageEngine(null, configuration.getDatabasePrefix() + "_test_connection", "test_connection")
                    .testConnection();
            status.setVariantStorageId(storageEngineFactory.getVariantStorageEngine().getStorageEngineId());
            status.setVariantStorageStatus(HealthCheckStatus.Status.OK);
        } catch (Exception e) {
            status.setVariantStorageStatus(HealthCheckStatus.Status.KO);
            healthy = false;
            errorMsg.append(e.getMessage());
            logger.error("Error reading variant storage status", e);
        }
        storageTime.stop();

        StopWatch solrEngineTime = StopWatch.createStarted();
        if (storageEngineFactory.getStorageConfiguration().getSearch().isActive()) {
            try {
                if (variantManager.isSolrAvailable()) {
                    status.setSolrStatus(HealthCheckStatus.Status.OK);
                } else {
                    errorMsg.append(", unable to connect with solr, ");
                    status.setSolrStatus(HealthCheckStatus.Status.KO);
                    healthy = false;
                }
            } catch (Exception e) {
                status.setSolrStatus(HealthCheckStatus.Status.KO);
                healthy = false;
                errorMsg.append(e.getMessage());
                logger.error("Error reading solr status", e);
            }
        } else {
            status.setSolrStatus(HealthCheckStatus.Status.NA);
        }
        solrEngineTime.stop();
        totalTime.stop();

        if (totalTime.getTime(TimeUnit.SECONDS) > 5) {
            logger.warn("Slow OpenCGA status: Updated time: {}. Catalog: {} , Storage: {} , Solr: {}",
                    totalTime.getTime(TimeUnit.MILLISECONDS) / 1000.0,
                    catalogMongoDBTime.getTime(TimeUnit.MILLISECONDS) / 1000.0,
                    storageTime.getTime(TimeUnit.MILLISECONDS) / 1000.0,
                    solrEngineTime.getTime(TimeUnit.MILLISECONDS) / 1000.0
            );
        }

        if (errorMsg.length() == 0) {
            status.setErrorMessage(null);
        } else {
            status.setErrorMessage(errorMsg.toString());
        }

        status.setCreationDate(LocalDateTime.now());
        status.setHealthy(healthy);
        cachedResult.set(status);
    }

}

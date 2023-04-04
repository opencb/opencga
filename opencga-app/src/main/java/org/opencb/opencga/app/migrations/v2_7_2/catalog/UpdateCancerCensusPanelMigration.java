package org.opencb.opencga.app.migrations.v2_7_2.catalog;

import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;

import java.io.InputStream;
import java.net.URL;
import java.util.Collections;

@Migration(id = "update_cancer_census_panel" ,
        description = "Update Cancer Census panel",
        version = "2.7.2",
        domain = Migration.MigrationDomain.CATALOG,
        language = Migration.MigrationLanguage.JAVA,
        date = 20230403
)
public class UpdateCancerCensusPanelMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        String host = configuration.getPanel().getHost();
        if (StringUtils.isEmpty(host)) {
            throw new CatalogException("Configuration of panel host missing. Please, consult with your administrator.");
        }
        if (!host.endsWith("/")) {
            host = host + "/";
        }
        URL url = new URL(host + "gene-census/gene-census.json");

        DiseasePanel panel;
        try (InputStream inputStream = url.openStream()) {
            panel = JacksonUtils.getDefaultObjectMapper().readValue(inputStream, DiseasePanel.class);
        }

        PanelUpdateParams updateParams = new PanelUpdateParams(null, panel.getId(), panel.getDescription(), null,
                panel.getSource(), panel.getCategories(), panel.getTags(), panel.getDisorders(), panel.getVariants(), panel.getGenes(),
                panel.getRegions(), panel.getStrs(), panel.getStats(), panel.getAttributes());

        Query query = new Query(PanelDBAdaptor.QueryParams.ID.key(), panel.getId());
        queryMongo(MongoDBAdaptorFactory.STUDY_COLLECTION, new Document(), Projections.include(Collections.singletonList("fqn")), document -> {
            String study = document.getString("fqn");
            if (!study.startsWith("opencga@")) {
                // Update panel if present
                try {
                    if (catalogManager.getPanelManager().count(study, query, token).getNumMatches() == 1) {
                        logger.info("Updating panel {} from study {}", panel.getId(), study);
                        catalogManager.getPanelManager().update(study, panel.getId(), updateParams, QueryOptions.empty(), token);
                    }
                } catch (CatalogException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

}

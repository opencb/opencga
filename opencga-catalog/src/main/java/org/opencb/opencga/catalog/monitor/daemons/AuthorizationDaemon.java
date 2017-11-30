package org.opencb.opencga.catalog.monitor.daemons;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.DBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.PermissionRules;
import org.opencb.opencga.core.models.Study;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AuthorizationDaemon extends MonitorParentDaemon {

    private StudyDBAdaptor studyDBAdaptor;
    private AuthorizationManager authorizationManager;

    public AuthorizationDaemon(int interval, String sessionId, CatalogManager catalogManager) throws CatalogDBException {
        super(interval, sessionId, catalogManager);
        this.studyDBAdaptor = dbAdaptorFactory.getCatalogStudyDBAdaptor();
        this.authorizationManager = catalogManager.getAuthorizationManager();
    }

    @Override
    public void run() {

        Query allStudies = new Query();
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(StudyDBAdaptor.QueryParams.PERMISSION_RULES.key(), StudyDBAdaptor.QueryParams.ALIAS.key(),
                                StudyDBAdaptor.QueryParams.ID.key()));

        while (!exit) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                if (!exit) {
                    e.printStackTrace();
                }
            }
            logger.info("----- AUTHORIZATION DAEMON -----", TimeUtils.getTimeMillis());

            try (DBIterator<Study> iterator = studyDBAdaptor.iterator(allStudies, options)) {
                while (iterator.hasNext()) {
                    applyPermissionRules(iterator.next());
                }
            } catch (Exception e) {
                logger.error("{}", e.getMessage(), e);
            }
        }
    }

    private void applyPermissionRules(Study study) {
        if (study.getPermissionRules().isEmpty()) {
            return;
        }

        logger.info("Analysing study {} ({})", study.getAlias(), study.getId());

        for (Map.Entry<Study.Entry, List<PermissionRules>> myMap : study.getPermissionRules().entrySet()) {
            Study.Entry entry = myMap.getKey();
            for (PermissionRules permissionRules : myMap.getValue()) {

                try {
                    if (permissionRules.getId().endsWith(DBAdaptor.INTERNAL_DELIMITER + "TODELETEANDRESTORE")) {
                        logger.info("Removing permission rule {} and removing applied permissions for {}",
                                permissionRules.getId().split(DBAdaptor.INTERNAL_DELIMITER)[0], entry);


                    } else if (permissionRules.getId().endsWith(DBAdaptor.INTERNAL_DELIMITER + "TODELETE")) {
                        logger.info("Removing permission rule {} for {}", permissionRules.getId().split(DBAdaptor.INTERNAL_DELIMITER)[0],
                                entry);


                    } else {
                        logger.info("Attempting to apply permission rule {} in {}", permissionRules.getId(), entry);
                        authorizationManager.applyPermissionRules(study.getId(), permissionRules, entry.getName(), "admin");
                    }

                } catch (CatalogException e) {
                    logger.error("{}", e.getMessage(), e);
                }
            }
        }
    }



}

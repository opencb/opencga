package org.opencb.opencga.catalog.utils;

import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Study;

import java.io.IOException;
import java.util.*;

/**
 * Created by pfurio on 08/06/16.
 */
public final class CatalogDemo {

    private CatalogDemo() {

    }

    /**
     * Populates the database with dummy data.
     *
     * @param catalogConfiguration Catalog configuration file.
     * @param force Used in the case where a database already exists with the same name. When force = true, it will override it.
     * @throws CatalogException when there is already a database with the same name and force is false.
     */
    public static void createDemoDatabase(CatalogConfiguration catalogConfiguration, boolean force) throws CatalogException {
        CatalogManager catalogManager = new CatalogManager(catalogConfiguration);
        if (catalogManager.existsCatalogDB()) {
            if (force) {
                catalogManager.deleteCatalogDB(force);
            } else {
//                throw new CatalogException("A database called " + catalogConfiguration.getDatabase().getDatabase() + " already exists");
                throw new CatalogException("A database called " + catalogManager.getCatalogDatabase() + " already exists");
            }
        }
        catalogManager.installCatalogDB();
        try {
            populateDatabase(catalogManager);
        } catch (IOException e) {
            throw new CatalogException(e.getMessage());
        }
    }

    private static void populateDatabase(CatalogManager catalogManager) throws CatalogException, IOException {
        // Create users
        Map<String, String> userSessions = new HashMap<>(5);
        for (int i = 1; i <= 5; i++) {
            String id = "user" + i;
            String name = "User" + i;
            String password = id + "_pass";
            String email = id + "@gmail.com";
            catalogManager.createUser(id, name, email, password, "organization", 2000L, null);
            userSessions.put(id, (String) catalogManager.login(id, password, "localhost").first().get("sessionId"));
        }

        // Create one project per user
        Map<String, Long> projects = new HashMap<>(5);
        for (Map.Entry<String, String> userSession : userSessions.entrySet()) {
            projects.put(userSession.getKey(), catalogManager.createProject("DefaultProject", "default",
                    "Description", "Organization", null, userSession.getValue()).first().getId());
        }

        // Create two studies per user
        Map<String, List<Long>> studies = new HashMap<>(5);
        for (Map.Entry<String, String> userSession : userSessions.entrySet()) {
            long projectId = projects.get(userSession.getKey());
            List<Long> studiesTmp = new ArrayList<>(2);
            for (int i = 1; i <= 2; i++) {
                String name = "Name of study" + i;
                String alias = "study" + i;
                studiesTmp.add(catalogManager.createStudy(projectId, name, alias, Study.Type.FAMILY, "Description of " + alias,
                        userSession.getValue()).first().getId());
            }
            studies.put(userSession.getKey(), studiesTmp);
        }

        /*
        SHARE STUDY1 OF USER1
         */
        long studyId = studies.get("user1").get(0);
        String sessionId = userSessions.get("user5");

        // user5 will have the role "admin"
        catalogManager.createStudyAcls(Long.toString(studyId), "user5", "", "admin", userSessions.get("user1"));
        // user5 will add the rest of users. user2, user3 and user4 go to group "members"
        catalogManager.createGroup(Long.toString(studyId), "members", "user2,user3,user4", sessionId);
//        // @members will have the role "analyst"
        catalogManager.createStudyAcls(Long.toString(studyId), "@members", "", "analyst", sessionId);
//        // Add anonymous user to the role "denyAll". Later we will give it permissions to see some concrete samples.
        catalogManager.createStudyAcls(Long.toString(studyId), "anonymous", "", "locked", sessionId);
    }

}

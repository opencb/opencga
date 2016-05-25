package org.opencb.opencga.app.demo;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.files.FileMetadataReader;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;
import org.opencb.opencga.catalog.utils.CatalogFileUtils;
import org.opencb.opencga.catalog.utils.CatalogSampleAnnotationsLoader;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created on 20/05/16
 *
 * @author Pedro Furio &lt;pedrofurio@gmail.com&gt;
 */
public class CatalogManagerDemo {

    private CatalogManagerDemo() {
    }

    public static void createDemoDatabase(CatalogManager catalogManager, boolean force)
            throws CatalogException, IOException, URISyntaxException, StorageManagerException {
        try {
            catalogManager.deleteCatalogDB(force);
        } catch (CatalogException e) {
            System.out.println("Could not delete database");
        }
        catalogManager.installCatalogDB();
        populateDatabase(catalogManager);
    }

    private static void populateDatabase(CatalogManager catalogManager)
            throws CatalogException, IOException, URISyntaxException, StorageManagerException {
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
            projects.put(userSession.getKey(), catalogManager.createProject(userSession.getKey(), "DefaultProject", "default",
                    "Description", "Organization", null, userSession.getValue()).first().getId());
        }

        // Create two studies per user
        Map <String, List<Long>> studies = new HashMap<>(5);
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
        catalogManager.shareStudy(studyId, "user5", "admin", userSessions.get("user1"));
        // user5 will add the rest of users. user2, user3 and user4 go to group "members"
        catalogManager.addUsersToGroup(studyId, "members", "user2,user3,user4", sessionId);
//        // @members will have the role "analyst"
        catalogManager.shareStudy(studyId, "@members", "analyst", sessionId);
//        // Add anonymous user to the role "denyAll". Later we will give it permissions to see some concrete samples.
        catalogManager.shareStudy(studyId, "anonymous", "locked", sessionId);

        /*
        CREATE FILES
         */
        // Add pedigree file
        File file = createPedigreeFile(catalogManager, studyId, sessionId);

        // Load samples using the pedigree file
        CatalogSampleAnnotationsLoader catalogSampleAnnotationsLoader = new CatalogSampleAnnotationsLoader(catalogManager);
        catalogSampleAnnotationsLoader.loadSampleAnnotations(file, null, sessionId);

        // Load a VCF file
        file = createVariantFile(catalogManager, studyId, sessionId);
        // TODO: Index variant file

    }

    private static File createPedigreeFile(CatalogManager catalogManager, long studyId, String sessionId)
            throws URISyntaxException, CatalogException, IOException, StorageManagerException {
        String path = "data/peds";
        Path inputFile = Paths.get(System.getenv("OPENCGA_HOME") + "/examples/20130606_g1k.ped");
        URI sourceUri = inputFile.toUri();
        File file = catalogManager.createFile(studyId, File.Format.PED, File.Bioformat.PEDIGREE,
                Paths.get(path, inputFile.getFileName().toString()).toString(), "Description", true, -1, sessionId).first();
        new CatalogFileUtils(catalogManager).upload(sourceUri, file, null, sessionId, false, false, false, false);
        FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(), sessionId, false);
        return file;
    }

    private static File createVariantFile(CatalogManager catalogManager, long studyId, String sessionId)
            throws URISyntaxException, CatalogException, IOException, StorageManagerException {
        String path = "data/vcfs";
        Path inputFile = Paths.get(System.getenv("OPENCGA_HOME")
                + "/examples/1k.chr1.phase3_shapeit2_mvncall_integrated_v5.20130502.genotypes.vcf.gz");
        URI sourceUri = inputFile.toUri();
        File file = catalogManager.createFile(studyId, File.Format.VCF, File.Bioformat.VARIANT,
                Paths.get(path, inputFile.getFileName().toString()).toString(), "Description", true, -1, sessionId).first();
        new CatalogFileUtils(catalogManager).upload(sourceUri, file, null, sessionId, false, false, false, false);
        FileMetadataReader.get(catalogManager).setMetadataInformation(file, null, new QueryOptions(), sessionId, false);
        return file;
    }

}

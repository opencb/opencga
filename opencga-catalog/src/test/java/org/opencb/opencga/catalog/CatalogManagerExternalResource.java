package org.opencb.opencga.catalog;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created on 05/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogManagerExternalResource extends ExternalResource {

    private CatalogManager catalogManager;
    private CatalogConfiguration catalogConfiguration;

    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected void before() throws Throwable {
        super.before();

        temporaryFolder.create();
        catalogConfiguration = CatalogConfiguration.load(getClass().getResource("/catalog-configuration-test.yml").openStream());
        catalogConfiguration.setDataDir(temporaryFolder.newFolder("sessions").toURI().toString());
        catalogConfiguration.setTempJobsDir(temporaryFolder.newFolder("jobs").toURI().toString());

        catalogManager = new CatalogManager(catalogConfiguration);
        try {
            catalogManager.deleteCatalogDB();
        } catch (Exception ignore) {}
        catalogManager.installCatalogDB();
    }

    @Override
    protected void after() {
        super.after();
        temporaryFolder.delete();
        try {
            catalogManager.close();
        } catch (CatalogException e) {
            throw new RuntimeException(e);
        }
    }

    public CatalogConfiguration getCatalogConfiguration() {
        return catalogConfiguration;
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }

    public TemporaryFolder getTemporaryFolder() {
        return temporaryFolder;
    }
}

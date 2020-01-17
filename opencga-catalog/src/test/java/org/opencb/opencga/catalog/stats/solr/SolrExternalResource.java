package org.opencb.opencga.catalog.stats.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.opencb.opencga.catalog.managers.CatalogManagerExternalResource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * @see org.opencb.opencga.storage.core.variant.solr.VariantSolrExternalResource
 */
public class SolrExternalResource extends CatalogManagerExternalResource {

    private static SolrClient solrClient;

    @Override
    public void before() throws Exception {
        super.before();

        Path rootDir = getOpencgaHome();

        // Copy configuration
        copyConfiguration("cohort-managed-schema", CatalogSolrManager.COHORT_CONF_SET);
        copyConfiguration("family-managed-schema", CatalogSolrManager.FAMILY_CONF_SET);
        copyConfiguration("file-managed-schema", CatalogSolrManager.FILE_CONF_SET);
        copyConfiguration("individual-managed-schema", CatalogSolrManager.INDIVIDUAL_CONF_SET);
        copyConfiguration("sample-managed-schema", CatalogSolrManager.SAMPLE_CONF_SET);

        String solrHome = rootDir.resolve("solr").toString();

        solrClient = create(solrHome, rootDir.resolve("solr/configsets").toString());

        CoreAdminRequest.Create request = new CoreAdminRequest.Create();
        request.setCoreName(getConfiguration().getDatabasePrefix() + "_" + CatalogSolrManager.COHORT_SOLR_COLLECTION);
        request.setConfigSet(CatalogSolrManager.COHORT_CONF_SET);
        request.process(solrClient);

        request.setCoreName(getConfiguration().getDatabasePrefix() + "_" + CatalogSolrManager.SAMPLE_SOLR_COLLECTION);
        request.setConfigSet(CatalogSolrManager.SAMPLE_CONF_SET);
        request.process(solrClient);

        request.setCoreName(getConfiguration().getDatabasePrefix() + "_" + CatalogSolrManager.INDIVIDUAL_SOLR_COLLECTION);
        request.setConfigSet(CatalogSolrManager.INDIVIDUAL_CONF_SET);
        request.process(solrClient);

        request.setCoreName(getConfiguration().getDatabasePrefix() + "_" + CatalogSolrManager.FILE_SOLR_COLLECTION);
        request.setConfigSet(CatalogSolrManager.FILE_CONF_SET);
        request.process(solrClient);

        request.setCoreName(getConfiguration().getDatabasePrefix() + "_" + CatalogSolrManager.FAMILY_SOLR_COLLECTION);
        request.setConfigSet(CatalogSolrManager.FAMILY_CONF_SET);
        request.process(solrClient);
    }

    private void copyConfiguration(String managedSchema, String confFolder) throws IOException {
        getResourceUri("solr/configsets/solrconfig.xml", "solr/configsets/" + confFolder + "/solrconfig.xml");
        getResourceUri("solr/" + managedSchema, "solr/configsets/" + confFolder + "/schema.xml");
        getResourceUri("solr/configsets/params.json", "solr/configsets/" + confFolder + "/params.json");
        getResourceUri("solr/configsets/protwords.txt", "solr/configsets/" + confFolder + "/protwords.txt");
        getResourceUri("solr/configsets/stopwords.txt", "solr/configsets/" + confFolder + "/stopwords.txt");
        getResourceUri("solr/configsets/synonyms.txt", "solr/configsets/" + confFolder + "/synonyms.txt");
        getResourceUri("solr/configsets/lang/stopwords_en.txt", "solr/configsets/" + confFolder + "/lang/stopwords_en.txt");
    }

    @Override
    public void after() {
        super.after();
        close(solrClient);
    }

    private void close(SolrClient solrClient) {
        try {
            ((MyEmbeddedSolrServer) solrClient).realClose();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            solrClient = null;
        }
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }
//
//    /**
//     * Cleans the given solrHome directory and creates a new EmbeddedSolrServer.
//     *
//     * @param solrHome the Solr home directory to use
//     * @param configSetHome the directory containing config sets
//     * @param coreName the name of the core, must have a matching directory in configHome
//     *
//     * @return an EmbeddedSolrServer with a core created for the given coreName
//     * @throws IOException
//     */
//    public static SolrClient create(final String solrHome, final String configSetHome, final String coreName)
//            throws IOException, SolrServerException {
//        return create(solrHome, configSetHome, coreName, true);
//    }

    /**
     * @param solrHome the Solr home directory to use
     * @param configSetHome the directory containing config sets
     *
     * @return an EmbeddedSolrServer with a core created for the given coreName
     * @throws IOException
     */
    private SolrClient create(final String solrHome, final String configSetHome)
            throws IOException {

        final File solrHomeDir = new File(solrHome);
        if (!solrHomeDir.exists()) {
            solrHomeDir.mkdirs();
        }

        // Copy the solr.xml file to the solr home folder to be used
//        getResourceUri("solr/solr.xml", "solr/solr.xml");

        final SolrResourceLoader loader = new SolrResourceLoader(solrHomeDir.toPath());
        final Path configSetPath = Paths.get(configSetHome).toAbsolutePath();

        final NodeConfig config = new NodeConfig.NodeConfigBuilder("embeddedSolrServerNode", loader)
                .setConfigSetBaseDirectory(configSetPath.toString())
                .build();

        final EmbeddedSolrServer embeddedSolrServer = new MyEmbeddedSolrServer(config,
                getConfiguration().getDatabasePrefix() + "_" + CatalogSolrManager.SAMPLE_SOLR_COLLECTION);

        return embeddedSolrServer;
    }

    private URI getResourceUri(String resourceName, String targetName) throws IOException {
        Path rootDir = getOpencgaHome();
        Path resourcePath = rootDir.resolve(targetName);
        if (!resourcePath.getParent().toFile().exists()) {
            Files.createDirectories(resourcePath.getParent());
        }
        if (!resourcePath.toFile().exists()) {
            InputStream stream = SolrExternalResource.class.getClassLoader().getResourceAsStream(resourceName);
            Files.copy(stream, resourcePath, StandardCopyOption.REPLACE_EXISTING);
        }
        return resourcePath.toUri();
    }

    private static class MyEmbeddedSolrServer extends EmbeddedSolrServer {

        public MyEmbeddedSolrServer(NodeConfig nodeConfig, String defaultCoreName) {
            super(nodeConfig, defaultCoreName);
        }

        @Override
        public void close() throws IOException {
        }

        private void realClose() throws IOException {
            super.close();
        }
    }
}

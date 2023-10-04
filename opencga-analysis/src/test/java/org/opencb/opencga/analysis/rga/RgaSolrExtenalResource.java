package org.opencb.opencga.analysis.rga;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.NodeConfig;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.StorageConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

public class RgaSolrExtenalResource extends ExternalResource {

    public String coreName = "opencga_rga_test";

    private SolrClient solrClient;
    protected boolean embeded = true;
    private Class<?> testClass;

    public RgaSolrExtenalResource() {
        this(true);
    }

    public RgaSolrExtenalResource(boolean embeded) {
        this.embeded = embeded;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        testClass = description.getTestClass();
        return super.apply(base, description);
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        Path rootDir = Paths.get("target/test-data", "junit-rga-solr-" + TimeUtils.getTimeMillis());
        Files.createDirectories(rootDir);

        String mainConfigSet = "opencga-rga-configset-" + GitRepositoryState.getInstance().getBuildVersion();
        String auxConfigSet = "opencga-rga-aux-configset-" + GitRepositoryState.getInstance().getBuildVersion();
        copyConfigSetConfiguration(mainConfigSet, "managed-schema", rootDir);
        copyConfigSetConfiguration(auxConfigSet, "aux-managed-schema", rootDir);

        String solrHome = rootDir.resolve("solr").toString();

        if (embeded) {
            solrClient = create(solrHome, rootDir.resolve("configsets").toString(), coreName);
        } else {
            String host = "http://localhost:8983/solr";
            int timeout = 5000;

            SolrManager solrManager = new SolrManager(host, "core", timeout);
            if (!solrManager.existsCore(coreName)) {
                solrManager.createCore(coreName, mainConfigSet);
            }

            this.solrClient = solrManager.getSolrClient();
        }
    }

    @Override
    protected void after() {
        super.after();
        try {
            if (solrClient != null) {
                if (embeded) {
                    ((RgaSolrExtenalResource.MyEmbeddedSolrServer) solrClient).realClose();
                } else {
                    solrClient.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            solrClient = null;
        }
    }

    private void copyConfigSetConfiguration(String configSet, String managedSchemaFile, Path rootDir) throws IOException {
        // Copy configuration
        getResourceUri("configsets/variantsCollection/solrconfig.xml", "configsets/" + configSet + "/solrconfig.xml", rootDir);
        getResourceUri("rga/" + managedSchemaFile, "configsets/" + configSet + "/managed-schema", rootDir);
        getResourceUri("configsets/variantsCollection/params.json", "configsets/" + configSet + "/params.json", rootDir);
        getResourceUri("configsets/variantsCollection/protwords.txt", "configsets/" + configSet + "/protwords.txt", rootDir);
        getResourceUri("configsets/variantsCollection/stopwords.txt", "configsets/" + configSet + "/stopwords.txt", rootDir);
        getResourceUri("configsets/variantsCollection/synonyms.txt", "configsets/" + configSet + "/synonyms.txt", rootDir);
        getResourceUri("configsets/variantsCollection/lang/stopwords_en.txt", "configsets/" + configSet + "/lang/stopwords_en.txt", rootDir);
    }

    public RgaEngine configure(StorageConfiguration storageConfiguration) {
        RgaEngine rgaEngine = new RgaEngine(storageConfiguration);
        rgaEngine.setSolrManager(new SolrManager(solrClient, "localhost", "core"));
        return rgaEngine;
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }

    /**
     * Cleans the given solrHome directory and creates a new EmbeddedSolrServer.
     *
     * @param solrHome the Solr home directory to use
     * @param configSetHome the directory containing config sets
     * @param coreName the name of the core, must have a matching directory in configHome
     *
     * @return an EmbeddedSolrServer with a core created for the given coreName
     * @throws IOException
     */
    public static SolrClient create(final String solrHome, final String configSetHome, final String coreName)
            throws IOException, SolrServerException {
        return create(solrHome, configSetHome, coreName, true);
    }

    /**
     * @param solrHome the Solr home directory to use
     * @param configSetHome the directory containing config sets
     * @param coreName the name of the core, must have a matching directory in configHome
     * @param cleanSolrHome if true the directory for solrHome will be deleted and re-created if it already exists
     *
     * @return an EmbeddedSolrServer with a core created for the given coreName
     * @throws IOException
     */
    public static SolrClient create(final String solrHome, final String configSetHome, final String coreName, final boolean cleanSolrHome)
            throws IOException, SolrServerException {

        final File solrHomeDir = new File(solrHome);
        if (solrHomeDir.exists()) {
            if (cleanSolrHome) {
                FileUtils.deleteDirectory(solrHomeDir);
                solrHomeDir.mkdirs();
            }
        } else {
            solrHomeDir.mkdirs();
        }

        System.setProperty("solr.solr.home", solrHomeDir.toPath().toAbsolutePath().normalize().toString());

//        final SolrResourceLoader loader = new SolrResourceLoader(solrHomeDir.toPath());
        final Path configSetPath = Paths.get(configSetHome).toAbsolutePath();

        final NodeConfig config = new NodeConfig.NodeConfigBuilder("embeddedSolrServerNode", solrHomeDir.toPath())
                .setConfigSetBaseDirectory(configSetPath.toString())
                .build();

        final EmbeddedSolrServer embeddedSolrServer = new RgaSolrExtenalResource.MyEmbeddedSolrServer(config, coreName);

//        final CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
//        createRequest.setCoreName(coreName);
//        createRequest.setConfigSet(coreName);
//        embeddedSolrServer.request(createRequest);

        return embeddedSolrServer;
    }

    private static class MyEmbeddedSolrServer extends EmbeddedSolrServer {
        public MyEmbeddedSolrServer(NodeConfig config, String coreName) {
            super(config, coreName);
        }

        @Override
        public void close() throws IOException {
        }

        private void realClose() throws IOException {
            super.close();
        }
    }
}

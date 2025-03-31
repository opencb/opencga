package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.core.GitUtils;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.CollectionPrefixUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.NodeConfig;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import static com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine.*;
import static org.junit.Assert.fail;

public class CvdbSolrExtenalResource extends ExternalResource {

    private SolrClient solrClient;

    protected boolean embeded;
    private String organizationId;
    private String projectId;

    private String collectionPrefix;

    private String solrHost = "http://localhost:8983/solr"; // "localhost:2181"; //"http://localhost:8983/solr";
    private String solrMode = "core"; //"cloud";
    private int solrTimeout = 30000;

    private static Path rootDir;

    private Configuration configuration;
    private EnterpriseConfiguration enterpriseConfiguration;

    public CvdbSolrExtenalResource(boolean embeded, String organizationId, String projectId, String collectionPrefix) {
        this.embeded = embeded;
        this.organizationId = organizationId;
        this.projectId = projectId;
        this.collectionPrefix = collectionPrefix;

        try {
            this.configuration = Configuration.load(CvdbSolrExtenalResource.class.getResourceAsStream("/configuration-test.yml"));
            this.enterpriseConfiguration = EnterpriseConfiguration.load(CvdbSolrExtenalResource.class.getResourceAsStream("/enterprise-configuration.yml"));

            System.out.println("this.configuration.getDatabasePrefix() = " + this.configuration.getDatabasePrefix());
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Override
    protected void before() throws Throwable {
        super.before();

//        Path rootDir = getTmpRootDir();

        String caConfigSet = "opencga-ca-configset-" + GitUtils.getEnterprise().getBuildVersion();
        String ciConfigSet = "opencga-ci-configset-" + GitUtils.getEnterprise().getBuildVersion();
        String cvConfigSet = "opencga-cv-configset-" + GitUtils.getEnterprise().getBuildVersion();
        String cveConfigSet = "opencga-cve-configset-" + GitUtils.getEnterprise().getBuildVersion();
        copyConfigSetConfiguration(caConfigSet, "ca-managed-schema");
        copyConfigSetConfiguration(ciConfigSet, "ci-managed-schema");
        copyConfigSetConfiguration(cvConfigSet, "cv-managed-schema");
        copyConfigSetConfiguration(cveConfigSet, "cve-managed-schema");

        String solrHome = rootDir.resolve("solr").toString();

        if (embeded) {
            solrClient = create(solrHome, rootDir.resolve("configsets").toString(),
                    CollectionPrefixUtils.getCollectionName(collectionPrefix, CLINICAL_ANALYSES_COLLECTION_SUFFIX)
                            + "," + CollectionPrefixUtils.getCollectionName(collectionPrefix, INTERPRETATIONS_COLLECTION_SUFFIX)
                            + "," + CollectionPrefixUtils.getCollectionName(collectionPrefix, CLINICAL_VARIANTS_COLLECTION_SUFFIX)
                            + "," + CollectionPrefixUtils.getCollectionName(collectionPrefix, CLINICAL_VARIANT_EVIDENCES_COLLECTION_SUFFIX));
        } else {
            SolrManager solrManager = new SolrManager(solrHost, solrMode, solrTimeout);
            this.solrClient = solrManager.getSolrClient();
        }
    }

    @Override
    protected void after() {
        super.after();
        try {
            if (solrClient != null) {
                if (embeded) {
                    ((CvdbSolrExtenalResource.MyEmbeddedSolrServer) solrClient).realClose();
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

    private void copyConfigSetConfiguration(String configSet, String managedSchemaFile) throws IOException {
        // Copy configuration
        getResourceUri("configset/solrconfig.xml", "configsets/" + configSet + "/conf/solrconfig.xml");
        getResourceUri("configset/params.json", "configsets/" + configSet + "/conf/params.json");
        getResourceUri("configset/protwords.txt", "configsets/" + configSet + "/conf/protwords.txt");
        getResourceUri("configset/stopwords.txt", "configsets/" + configSet + "/conf/stopwords.txt");
        getResourceUri("configset/synonyms.txt", "configsets/" + configSet + "/conf/synonyms.txt");
        getResourceUri("configset/lang/stopwords_en.txt", "configsets/" + configSet + "/conf/lang/stopwords_en.txt");

        getResourceUri("schemas/" + managedSchemaFile, "configsets/" + configSet + "/conf/managed-schema");
    }

    public CvdbSolrEngine configure() {
        CvdbSolrEngine cvdbEngine = new CvdbSolrEngine(configuration, enterpriseConfiguration.getCvdb());
        cvdbEngine.setSolrManager(new SolrManager(solrClient, solrHost, solrMode));
        return cvdbEngine;
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

        final Path configSetPath = Paths.get(configSetHome).toAbsolutePath();

        final NodeConfig config = new NodeConfig.NodeConfigBuilder("embeddedSolrServerNode", solrHomeDir.toPath())
                .setConfigSetBaseDirectory(configSetPath.toString())
                .build();

        final EmbeddedSolrServer embeddedSolrServer = new CvdbSolrExtenalResource.MyEmbeddedSolrServer(config, coreName);

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


    public static URI getResourceUri(String resourceName) throws IOException {
        return getResourceUri(resourceName, resourceName);
    }

    public static URI getResourceUri(String resourceName, String targetName) throws IOException {
        return getResourceUri(resourceName, targetName, getTmpRootDir());
    }

    public static URI getResourceUri(String resourceName, String targetName, Path rootDir) throws IOException {
        Path resourcePath = rootDir.resolve(targetName).toAbsolutePath();
        if (!resourcePath.getParent().toFile().exists()) {
            Files.createDirectories(resourcePath.getParent());
        }
        if (!resourcePath.toFile().exists()) {
            InputStream stream = CvdbSolrExtenalResource.class.getClassLoader().getResourceAsStream(resourceName);
            Assert.assertNotNull(resourceName, stream);
            Files.copy(stream, resourcePath, StandardCopyOption.REPLACE_EXISTING);
        }
        return resourcePath.toUri();
    }

    public static Path getTmpRootDir() throws IOException {
        if (rootDir == null) {
            newRootDir();
        }
        return rootDir;
    }

    public static URI getPlatinumFile(int fileId) throws IOException {
        String fileName;
        if (fileId < 17) {
            fileName = "1K.end.platinum-genomes-vcf-NA" + (fileId + 12877) + "_S1.genome.vcf.gz";
        } else if (fileId >= 12877 && fileId <= 12893){
            fileName = "1K.end.platinum-genomes-vcf-NA" + fileId + "_S1.genome.vcf.gz";
        } else {
            throw new IllegalArgumentException("Unknown platinum file " + fileId);
        }
        return getResourceUri("platinum/" + fileName);
    }


    private static void newRootDir() throws IOException {
        rootDir = Paths.get("target/test-data", "junit-opencga-storage-" + TimeUtils.getTimeMillis() + "_" + RandomStringUtils.randomAlphabetic(3));
        Files.createDirectories(rootDir);
    }
}

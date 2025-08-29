/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant.solr;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.SearchConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageBaseTest;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.getResourceUri;

/**
 * +
 * Created on 30/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSolrExternalResource extends ExternalResource {

    public static final String CONFIG_SET = "opencga_variants";

    private MySolrClient solrClient;
    private MiniSolrCloudCluster miniSolrCloudCluster;
    protected boolean embeded = true;
    private ZkTestServer zkTestServer;
    private Path rootDir;

    public VariantSolrExternalResource() {
        this(true);
    }

    public VariantSolrExternalResource(VariantStorageBaseTest baseTest) {
        this(true);
        try {
            rootDir = baseTest.getTmpRootDir().resolve("solr");
        } catch (IOException e) {
            throw new RuntimeException("Could not create temporary root directory", e);
        }
    }

    public VariantSolrExternalResource(boolean embeded) {
        this.embeded = embeded;
        rootDir = Paths.get("target/test-data", "junit-variant-solr-" + TimeUtils.getTimeMillis());
    }

    @Override
    public void before() throws Exception {
        Files.createDirectories(rootDir);

        String configSet = CONFIG_SET;

        // Copy configuration
        getResourceUri("configsets/variantsCollection/solrconfig.xml", "configsets/" + configSet + "/solrconfig.xml", rootDir);
        getResourceUri("managed-schema", "configsets/" + configSet + "/managed-schema", rootDir);
        getResourceUri("configsets/variantsCollection/params.json", "configsets/" + configSet + "/params.json", rootDir);
        getResourceUri("configsets/variantsCollection/protwords.txt", "configsets/" + configSet + "/protwords.txt", rootDir);
        getResourceUri("configsets/variantsCollection/stopwords.txt", "configsets/" + configSet + "/stopwords.txt", rootDir);
        getResourceUri("configsets/variantsCollection/synonyms.txt", "configsets/" + configSet + "/synonyms.txt", rootDir);
        getResourceUri("configsets/variantsCollection/lang/stopwords_en.txt", "configsets/" + configSet + "/lang/stopwords_en.txt", rootDir);

        String solrHome = rootDir.resolve("solr").toString();

        if (embeded) {
            create(solrHome, rootDir.resolve("configsets").toString());
            solrClient = new MySolrClient(miniSolrCloudCluster.getSolrClient());
        } else {
            String host = "http://localhost:8983/solr";
            int timeout = 5000;

            SolrManager solrManager = new SolrManager(host, "cloud", timeout);

            this.solrClient = new MySolrClient(solrManager.getSolrClient());
        }
    }

    @Override
    public void after() {
        super.after();
        try {
            if (solrClient != null) {
                if (embeded) {
                    solrClient.actuallyClose();
                    miniSolrCloudCluster.shutdown();
                    zkTestServer.shutdown();
                } else {
                    solrClient.actuallyClose();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            solrClient = null;
        }
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }

    public VariantSearchManager configure(VariantStorageEngine variantStorageEngine) throws StorageEngineException {
        SearchConfiguration searchConfiguration = variantStorageEngine.getConfiguration().getSearch();
        searchConfiguration.setMode("cloud");
        searchConfiguration.setActive(true);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();
        String host;
        if (embeded) {
            host = miniSolrCloudCluster.getZkClient().getZkServerAddress();
            searchConfiguration.setConfigSet(CONFIG_SET);
        } else {
            host = "http://localhost:8983";
        }
        variantSearchManager.initSolr(searchConfiguration, new SolrManager(solrClient, host, "cloud",
                searchConfiguration.getTimeout()));
        return variantSearchManager;
    }

    public void clearCollections() throws Exception {
        System.out.println("Clearing Solr collections");
        int i = 0;
        for (String collection : CollectionAdminRequest.listCollections(solrClient)) {
            i++;
            System.out.println("Deleting collection " + collection);
            CollectionAdminRequest.deleteCollection(collection).process(solrClient);
        }
        System.out.println("Collections cleared : " + i);
    }

    public void printCollections(Path outdir) throws SolrServerException, IOException {
        for (String collection : CollectionAdminRequest.listCollections(solrClient)) {
            printCollection(collection, outdir);
        }
    }

    public void printCollection(String collection, Path outdir) throws SolrServerException, IOException {
        Path output = outdir.resolve("solr." + collection + ".txt");
        System.out.println("Printing collection " + collection + " to " + output.toAbsolutePath());
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(output))) {
            for (SolrDocument result : solrClient.query(collection, new SolrQuery().setQuery("*:*").setRows(10000)).getResults()) {
                out.writeBytes(((ObjectMap) result.toMap(new ObjectMap())).toJson());
                out.writeBytes("\n");
            }
        }

    }

    /**
     * Cleans the given solrHome directory and creates a new EmbeddedSolrServer.
     *
     * @param solrHome the Solr home directory to use
     * @param configSetHome the directory containing config sets
     *
     * @return a MiniSolrCloudCluster
     * @throws IOException
     */
    private void create(final String solrHome, final String configSetHome)
            throws IOException {
        create(solrHome, configSetHome, true);
    }

    /**
     * @param solrHome the Solr home directory to use
     * @param configSetHome the directory containing config sets
     * @param cleanSolrHome if true the directory for solrHome will be deleted and re-created if it already exists
     *
     * @return a MiniSolrCloudCluster
     * @throws IOException
     */
    private void create(final String solrHome, final String configSetHome, final boolean cleanSolrHome)
            throws IOException {

        final File solrHomeDir = new File(solrHome);
        if (solrHomeDir.exists()) {
            if (cleanSolrHome) {
                FileUtils.deleteDirectory(solrHomeDir);
                solrHomeDir.mkdirs();
            }
        } else {
            solrHomeDir.mkdirs();
        }
        final File zkDir = new File(solrHomeDir, "zookeeper");
        if (zkDir.exists()) {
            if (cleanSolrHome) {
                // Delete the zookeeper directory if it exists
                FileUtils.deleteDirectory(zkDir);
                zkDir.mkdirs();
            }
        } else {
            zkDir.mkdirs();
        }

        System.setProperty("solr.solr.home", solrHomeDir.toPath().toAbsolutePath().normalize().toString());

//        final SolrResourceLoader loader = new SolrResourceLoader(solrHomeDir.toPath());
        final Path configSetPath = Paths.get(configSetHome).toAbsolutePath();

        try {

            zkTestServer = new ZkTestServer(zkDir.toPath().toAbsolutePath());
            zkTestServer.run();

            JettyConfig jettyConfig = JettyConfig.builder()
//                    .setPort(8989) // if you want multiple servers in the solr cloud comment it out.
                    .setContext("/solr")
                    .stopAtShutdown(true)
                    .withServlets(new HashMap<>())
                    .withSSLConfig(null)
                    .build();

            miniSolrCloudCluster = new MiniSolrCloudCluster(2, solrHomeDir.toPath(), MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML, jettyConfig, zkTestServer);
            miniSolrCloudCluster.uploadConfigSet(configSetPath.resolve(CONFIG_SET), CONFIG_SET);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class MySolrClient extends SolrClient {
        private final SolrClient actualSolrClient;

        public MySolrClient(SolrClient actualSolrClient) {
            this.actualSolrClient = actualSolrClient;
        }

        @Override
        public NamedList<Object> request(SolrRequest request, String collection) throws SolrServerException, IOException {
            return actualSolrClient.request(request, collection);
        }

        @Override
        public void close() throws IOException {
            // Do not close the actual SolrClient, as it is managed by MiniSolrCloudCluster
        }

        public void actuallyClose() throws IOException {
            actualSolrClient.close();
        }
    }
}

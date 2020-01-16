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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.*;

/**
 * +
 * Created on 30/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantSolrExternalResource extends ExternalResource {

    public String coreName = DB_NAME;

    private SolrClient solrClient;
    protected boolean embeded = true;

    public VariantSolrExternalResource() {
        this(true);
    }

    public VariantSolrExternalResource(boolean embeded) {
        this.embeded = embeded;
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        Path rootDir = getTmpRootDir();

        String configSet = "OpenCGAConfigSet";
        // Copy configuration
        getResourceUri("configsets/variantsCollection/solrconfig.xml", "configsets/" + configSet + "/solrconfig.xml");
        getResourceUri("solr/variant/managed-schema", "configsets/" + configSet + "/managed-schema");
        getResourceUri("configsets/variantsCollection/params.json", "configsets/" + configSet + "/params.json");
        getResourceUri("configsets/variantsCollection/protwords.txt", "configsets/" + configSet + "/protwords.txt");
        getResourceUri("configsets/variantsCollection/stopwords.txt", "configsets/" + configSet + "/stopwords.txt");
        getResourceUri("configsets/variantsCollection/synonyms.txt", "configsets/" + configSet + "/synonyms.txt");
        getResourceUri("configsets/variantsCollection/lang/stopwords_en.txt", "configsets/" + configSet + "/lang/stopwords_en.txt");

        String solrHome = rootDir.resolve("solr").toString();

        if (embeded) {
            solrClient = create(solrHome, rootDir.resolve("configsets").toString(), coreName);
        } else {
            String host = "http://localhost:8983/solr";
            int timeout = 5000;

            SolrManager solrManager = new SolrManager(host, "core", timeout);
            if (!solrManager.existsCore(coreName)) {
                solrManager.createCore(coreName, configSet);
            }

            this.solrClient = solrManager.getSolrClient();
        }
    }

    @Override
    protected void after() {
        super.after();
        try {
            if (embeded) {
                ((MyEmbeddedSolrServer) solrClient).realClose();
            } else {
                solrClient.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            solrClient = null;
        }
    }

    public SolrClient getSolrClient() {
        return solrClient;
    }

    public VariantSearchManager configure(VariantStorageEngine variantStorageEngine) throws StorageEngineException {
        variantStorageEngine.getConfiguration().getSearch().setMode("core");
        variantStorageEngine.getConfiguration().getSearch().setActive(true);
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();
        variantSearchManager.setSolrManager(new SolrManager(solrClient, "localhost", "core",
                variantStorageEngine.getConfiguration().getSearch().getTimeout()));
        return variantSearchManager;
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

        final SolrResourceLoader loader = new SolrResourceLoader(solrHomeDir.toPath());
        final Path configSetPath = Paths.get(configSetHome).toAbsolutePath();

        final NodeConfig config = new NodeConfig.NodeConfigBuilder("embeddedSolrServerNode", loader)
                .setConfigSetBaseDirectory(configSetPath.toString())
                .build();

        final EmbeddedSolrServer embeddedSolrServer = new MyEmbeddedSolrServer(config, coreName);

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

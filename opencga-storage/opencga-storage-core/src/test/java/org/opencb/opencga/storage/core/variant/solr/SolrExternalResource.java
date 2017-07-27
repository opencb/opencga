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
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.*;

/**
 * Created on 30/06/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SolrExternalResource extends ExternalResource {

    public String coreName = DB_NAME;

    private SolrClient solrClient;

    @Override
    protected void before() throws Throwable {
        super.before();

        Path rootDir = getTmpRootDir();

        String confFolder = VariantSearchManager.CONF_SET;
        // Copy configuration
        getResourceUri("configsets/variantsCollection/solrconfig.xml", "configsets/" + confFolder + "/solrconfig.xml");
        getResourceUri("configsets/variantsCollection/managed-schema", "configsets/" + confFolder + "/managed-schema");
        getResourceUri("configsets/variantsCollection/params.json", "configsets/" + confFolder + "/params.json");
        getResourceUri("configsets/variantsCollection/protwords.txt", "configsets/" + confFolder + "/protwords.txt");
        getResourceUri("configsets/variantsCollection/stopwords.txt", "configsets/" + confFolder + "/stopwords.txt");
        getResourceUri("configsets/variantsCollection/synonyms.txt", "configsets/" + confFolder + "/synonyms.txt");
        getResourceUri("configsets/variantsCollection/lang/stopwords_en.txt", "configsets/" + confFolder + "/lang/stopwords_en.txt");

        String solrHome = rootDir.resolve("solr").toString();

        solrClient = create(solrHome, rootDir.resolve("configsets").toString(), coreName);

    }

    @Override
    protected void after() {
        super.after();
        try {
            solrClient.close();
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
        VariantSearchManager variantSearchManager = variantStorageEngine.getVariantSearchManager();
        variantSearchManager.setSolrClient(getSolrClient());
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

        final EmbeddedSolrServer embeddedSolrServer = new EmbeddedSolrServer(config, coreName);

//        final CoreAdminRequest.Create createRequest = new CoreAdminRequest.Create();
//        createRequest.setCoreName(coreName);
//        createRequest.setConfigSet(coreName);
//        embeddedSolrServer.request(createRequest);

        return embeddedSolrServer;
    }
}

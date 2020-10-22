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

package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class InternalCommandExecutor extends CommandExecutor {

    protected CatalogManager catalogManager;
    protected StorageEngineFactory storageEngineFactory;
    protected ToolRunner toolRunner;

    public InternalCommandExecutor(GeneralCliOptions.CommonCommandOptions options) {
        super(options);
    }

    protected void configure() throws IllegalAccessException, ClassNotFoundException, InstantiationException, CatalogException {

        //  Creating CatalogManager
        catalogManager = new CatalogManager(configuration);

        // Creating StorageManagerFactory
        storageEngineFactory = StorageEngineFactory.get(storageConfiguration);

        toolRunner = new ToolRunner(appHome, catalogManager, storageEngineFactory);
    }

    protected Map<Long, String> getStudyIds(String sessionId) throws CatalogException {
        return catalogManager.getStudyManager().get(new Query(), new QueryOptions("include", "projects.studies.id,projects.studies" +
                ".alias"), sessionId)
                .getResults()
                .stream()
                .collect(Collectors.toMap(Study::getUid, Study::getId));
    }

}

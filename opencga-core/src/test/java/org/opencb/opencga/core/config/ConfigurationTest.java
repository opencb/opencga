/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.core.config;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.models.job.JobRunDockerParams;
import org.opencb.opencga.core.models.job.JobRunParams;
import org.opencb.opencga.core.models.variant.AnnotationVariantQueryParams;
import org.opencb.opencga.core.models.variant.qc.SampleQcAnalysisParams;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * Created by imedina on 16/03/16.
 */
@Category(ShortTests.class)
public class ConfigurationTest {

    @Test
    public void testDefault() throws IOException {
        JobRunParams jobRunParams = new JobRunParams().setDocker(new JobRunDockerParams("repository", "tag", "token"));
        Map<String, Object> params = jobRunParams.toParams();
        System.out.println(params);

        SampleQcAnalysisParams qcParams = new SampleQcAnalysisParams().setVsQuery(new AnnotationVariantQueryParams().setBiotype("biotype"));
        Map<String, Object> params1 = qcParams.toParams();
        System.out.println(params1);

        Configuration configuration = new Configuration();

        configuration.setLogLevel("INFO");

        configuration.setWorkspace("/opt/opencga/sessions");
        configuration.setMonitor(new Monitor());
        configuration.getAnalysis().setExecution(new Execution());

        configuration.setHooks(Collections.singletonMap("organization@project:study", Collections.singletonMap("file",
                Collections.singletonList(
                        new HookConfiguration("name", "~*SV*", HookConfiguration.Stage.CREATE, HookConfiguration.Action.ADD, "tags", "SV")
        ))));

        Email emailServer = new Email("localhost", "", "", "", "", false);
        configuration.setEmail(emailServer);

        DatabaseCredentials databaseCredentials = new DatabaseCredentials(Arrays.asList("localhost"), "admin", "");
        Catalog catalog = new Catalog();
        catalog.setDatabase(databaseCredentials);
        configuration.setCatalog(catalog);

        Audit audit = new Audit("", 20000000, 100);
        configuration.setAudit(audit);

        ServerConfiguration serverConfiguration = new ServerConfiguration();
        RestServerConfiguration rest = new RestServerConfiguration(1000);
        GrpcServerConfiguration grpc = new GrpcServerConfiguration(1001);
        serverConfiguration.setGrpc(grpc);
        serverConfiguration.setRest(rest);

        configuration.setServer(serverConfiguration);

//        CellBaseConfiguration cellBaseConfiguration = new CellBaseConfiguration(Arrays.asList("localhost"), "v3",
// new DatabaseCredentials(Arrays.asList("localhost"), "user", "password"));
//        QueryServerConfiguration queryServerConfiguration = new QueryServerConfiguration(61976, Arrays.asList("localhost"));
//
//        catalogConfiguration.setDefaultStorageEngineId("mongodb");
//
//        catalogConfiguration.setCellbase(cellBaseConfiguration);
//        catalogConfiguration.setServer(queryServerConfiguration);
//
//        catalogConfiguration.getStorageEngines().add(storageEngineConfiguration1);
//        catalogConfiguration.getStorageEngines().add(storageEngineConfiguration2);

        Path outdir = Paths.get("target/test-data", "junit-opencga-" +
                new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss.SSS").format(new Date()));
        Files.createDirectories(outdir);
        configuration.serialize(Files.newOutputStream(outdir.resolve("configuration-test.yml").toFile().toPath()));
    }

    @Test
    public void testLoad() throws Exception {
//        URL url = new URL("http://resources.opencb.org/opencb/opencga/disease-panels/sources.txt");
//        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), Charset.defaultCharset()));
//
//        Set<String> sources = new HashSet<>();
//        String line;
//        while((line = reader.readLine()) != null) {
//            sources.add(line);
//        }
//        System.out.println(sources);
//
//        File file = new File(url.toURI());
//        System.out.println(file.list());


        String path = "data/toto.txt/toto.txt";
        System.out.println("Paths.get(path).getParent() = " + Paths.get(path).getParent());
        System.out.println("Paths.get(path).getFileName() = " + Paths.get(path).getFileName());

        Configuration configuration = Configuration
                .load(getClass().getResource("/configuration-test.yml").openStream());
        System.out.println("catalogConfiguration = " + configuration);
    }
}
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

package org.opencb.opencga.client.config;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 04/05/16.
 */
@Category(ShortTests.class)
public class ClientConfigurationTest {

    @Test
    public void testDefault() {

        ClientConfiguration config = new ClientConfiguration();
        List<HostConfig> hostConfigs = new ArrayList<>();
        hostConfigs.add(new HostConfig("opencga", "localhost:9090/opencga"));
        RestConfig restConfig = new RestConfig(hostConfigs, true, new QueryRestConfig(200, 2000));
        GrpcConfig grpcConfig = new GrpcConfig("localhost:9091");

        config.setRest(restConfig);
        config.setGrpc(grpcConfig);

        try {
            config.serialize(new FileOutputStream("/tmp/client-configuration-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testLoad() throws Exception {
        ClientConfiguration storageConfiguration =
                ClientConfiguration.load(getClass().getResource("/client-configuration.yml").openStream());
        System.out.println("clientConfiguration = " + storageConfiguration);
    }
}
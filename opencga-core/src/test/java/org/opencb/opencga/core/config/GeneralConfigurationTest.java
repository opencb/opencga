/*
 * Copyright 2015-2016 OpenCB
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

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by imedina on 25/04/16.
 */
@Deprecated
public class GeneralConfigurationTest {

    @Test
    public void testDefault() {
        GeneralConfiguration generalConfiguration = new GeneralConfiguration();

        generalConfiguration.setRest(new RestServerConfiguration(9090));
        generalConfiguration.setGrpc(new GrpcServerConfiguration(9091));

        try {
            generalConfiguration.serialize(new FileOutputStream("/tmp/configuration-test.yml"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
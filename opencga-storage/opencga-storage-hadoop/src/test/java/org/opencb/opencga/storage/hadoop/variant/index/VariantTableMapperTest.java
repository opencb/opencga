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

package org.opencb.opencga.storage.hadoop.variant.index;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 02/08/2016.
 */
public class VariantTableMapperTest {
    @Test
    public void generateRegion() throws Exception {
        VariantTableMapper mapper = new VariantTableMapper();
        Set<Integer> integers = mapper.generateRegion(10, 20);
        assertEquals(new HashSet(Arrays.asList(10,11,12,13,14,15,16,17,18,19,20)),integers);
    }

    @Test
    public void generateRegionIndel() throws Exception {
        VariantTableMapper mapper = new VariantTableMapper();
        Set<Integer> integers = mapper.generateRegion(10, 9);
        assertEquals(new HashSet(Arrays.asList(10)),integers);
    }

}
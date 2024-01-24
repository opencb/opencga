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

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 24/01/24.
 */
public class Docker {

    // Important these keys have to match with the keys of the images map with the docker analysis configuration section
    public static final String OPENCGA_EXT_TOOLS_IMAGE_KEY = "opecgaExtTools";
    public static final String GATK_IMAGE_KEY = "gatk";
    public static final String PICARD_IMAGE_KEY = "picard";
    public static final String DEEPTOOLS_IMAGE_KEY = "deeptools";
    public static final String EXOMISER_IMAGE_KEY = "exomiser";
    public static final String RVTESTS_IMAGE_KEY = "rvtests";

    private Map<String, String> images;

    public Docker() {
        images = new HashMap<>();
    }

    public Docker(Map<String, String> images) {
        this.images = images;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Docker{");
        sb.append("images=").append(images);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, String> getImages() {
        return images;
    }

    public Docker setImages(Map<String, String> images) {
        this.images = images;
        return this;
    }
}


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

package org.opencb.opencga.core.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface AnalysisExecutor {

    /**
     * @return Analysis executor ID.
     */
    String id();

    /**
     * @return Analysis ID.
     */
    String analysis();

    /**
     * @return List of accepted sources.
     */
    Source source();

    /**
     * @return Required frameworks.
     */
    Framework framework();

    /**
     * @return Analysis executor description.
     */
    String description() default "";

    enum Source {
        VCF_FILE,
        PARQUET_FILE,
        MONGODB,
        HBASE,
        OPENCGA
    }

    enum Framework {
        ITERATOR,
        MAP_REDUCE,
        SPARK
    }
}

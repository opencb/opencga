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

package org.opencb.opencga.storage.benchmark.variant.generators;

import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class QueryGenerator {
    public static final String DATA_DIR = "dataDir";
    public static final String ARITY = "arity";
    protected Random random;

    private Logger logger = LoggerFactory.getLogger(getClass());
    private int arity;

    public void setUp(Map<String, String> params) {
        random = new Random(System.nanoTime());
        arity = Integer.parseInt(params.getOrDefault(ARITY, "1"));
    }

    protected void readCsvFile(Path path, Consumer<List<String>> consumer) {
        try (BufferedReader is = FileUtils.newBufferedReader(path)) {
            while (true) {
                String line = is.readLine();
                if (line == null) {
                    break;
                } else if (StringUtils.isBlank(line) || line.startsWith("#")) {
                    continue;
                }
                consumer.accept(Arrays.asList(line.split(",")));
            }
        } catch (IOException e) {
            logger.error("Error reading file " + path, e);
            throw Throwables.propagate(e);
        }
    }

    public abstract Query generateQuery(Query query);

    protected int getArity() {
        return arity;
    }

    public QueryGenerator setArity(int arity) {
        this.arity = arity;
        return this;
    }
}

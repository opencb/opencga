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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created on 10/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MultiQueryGenerator extends QueryGenerator {

    public static final String MULTI_QUERY = "multy-query";
    private List<QueryGenerator> generators;
    // param(extraParam)
    private Pattern pattern = Pattern.compile("(?<param>[^(]+)(\\((?<extraParam>[^)]+)\\))?");
    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        String query = params.get(MULTI_QUERY);
        generators = new ArrayList<>();
        for (String param : query.split(",")) {
            String extraParam = null;
            Integer arity = 1;

            Matcher matcher = pattern.matcher(param);
            if (matcher.find()) {
                param = matcher.group("param");
                extraParam = matcher.group("extraParam");
            }

            if (StringUtils.isNumeric(extraParam)) {
                arity = Integer.parseInt(extraParam);
            }

            QueryGenerator queryGenerator;
            switch (param.toLowerCase()) {
                case "region":
                    queryGenerator = new RegionQueryGenerator();
                    break;
                case "gene":
                    queryGenerator = new TermQueryGenerator.GeneQueryGenerator();
                    break;
                case "ct":
                    queryGenerator = new TermQueryGenerator.ConsequenceTypeQueryGenerator();
                    break;
                case "type":
                    queryGenerator = new TermQueryGenerator.TypeQueryGenerator();
                    break;
                case "study":
                case "studies":
                    queryGenerator = new TermQueryGenerator.StudyQueryGenerator();
                    break;
                case "biotype":
                    queryGenerator = new TermQueryGenerator.BiotypeQueryGenerator();
                    break;
                case "xrefs":
                    queryGenerator = new TermQueryGenerator.XrefQueryGenerator();
                    break;
                case "conservation":
                    queryGenerator = new ScoreQueryGenerator.ConservationQueryGenerator();
                    break;
                case "protein-substitution":
                    queryGenerator = new ScoreQueryGenerator.ProteinSubstQueryGenerator();
                    break;
                case "functional":
                case "cadd":
                    queryGenerator = new ScoreQueryGenerator.FunctionalScoreQueryGenerator();
                    break;
                default:
                    throw new IllegalArgumentException("Unknwon query param " + param);
            }
            logger.debug("Using sub query generator: " + queryGenerator.getClass() + " , arity = " + arity);
            params.put(ARITY, arity.toString());
            queryGenerator.setUp(params);
            generators.add(queryGenerator);
        }
    }

    @Override
    public Query generateQuery(Query query) {
        for (QueryGenerator generator : generators) {
            generator.generateQuery(query);
        }
        return query;
    }
}

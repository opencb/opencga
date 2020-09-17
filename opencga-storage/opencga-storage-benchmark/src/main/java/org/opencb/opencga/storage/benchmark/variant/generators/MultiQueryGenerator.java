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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;

import java.nio.file.Path;
import java.nio.file.Paths;
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

    public static final String MULTI_QUERY = "multi-query";
    public static final String RANDOM_QUERIES_FILE = "randomQueries.yml";
    private List<QueryGenerator> generators;
    private RandomQueries randomQueries;
    private Map<String, String> baseQueriesFromCLI;
    private Pattern pattern = Pattern.compile("(?<param>[^(]+)(\\((?<extraParam>[^)]+)\\))?");
    private Logger logger = LogManager.getLogger(getClass());
    private String query;

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        query = params.get(MULTI_QUERY);
        String queryFile = params.get(FILE);
        Path queryFilePath;

        if (queryFile == null || queryFile.isEmpty()) {
            queryFilePath = Paths.get(params.get(DATA_DIR), RANDOM_QUERIES_FILE);
        } else {
            queryFilePath = Paths.get(queryFile);
        }

        generators = new ArrayList<>();
        randomQueries = readYmlFile(queryFilePath, RandomQueries.class);
        baseQueriesFromCLI = getBaseQueryFromCLI(params);

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

            ConfiguredQueryGenerator queryGenerator;
            switch (param) {
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
                case "file":
                    queryGenerator = new TermQueryGenerator.FileQueryGenerator();
                    break;
                case "sample":
                    queryGenerator = new TermQueryGenerator.SampleQueryGenerator();
                    break;
                case "filter":
                    queryGenerator = new TermQueryGenerator.FilterQueryGenerator();
                    break;
                case "drug":
                    queryGenerator = new TermQueryGenerator.DrugQueryGenerator();
                    break;
                case "clinicalSignificance":
                    queryGenerator = new TermQueryGenerator.ClinicalSignificanceQueryGenerator();
                    break;
                case "transcriptFlag":
                case "transcriptionFlag":
                case "transcriptionFlags":
                    queryGenerator = new TermQueryGenerator.TranscriptionFlagsQueryGenerator();
                    break;
                case "includeSample":
                    queryGenerator = new TermQueryGenerator.IncludeSampleQueryGenerator();
                    break;
                case "includeFile":
                    queryGenerator = new TermQueryGenerator.IncludeFileQueryGenerator();
                    break;
                case "includeStudy":
                    queryGenerator = new TermQueryGenerator.IncludeStudyQueryGenerator();
                    break;
                case "conservation":
                    queryGenerator = new ScoreQueryGenerator.ConservationQueryGenerator(randomQueries);
                    break;
                case "proteinSubstitution":
                    queryGenerator = new ScoreQueryGenerator.ProteinSubstQueryGenerator(randomQueries);
                    break;
                case "populationFrequencyAlt":
                    queryGenerator = new ScoreQueryGenerator.PopulationFrequenciesAltQueryGenerator(randomQueries);
                    break;
                case "populationFrequencyRef":
                    queryGenerator = new ScoreQueryGenerator.PopulationFrequenciesRefQueryGenerator(randomQueries);
                    break;
                case "populationFrequencyMaf":
                    queryGenerator = new ScoreQueryGenerator.PopulationFrequenciesMafQueryGenerator(randomQueries);
                    break;
                case "qual":
                    queryGenerator = new ScoreQueryGenerator.QualQueryGenerator(randomQueries);
                    break;
                case "functionalScore":
                case "cadd":
                    queryGenerator = new ScoreQueryGenerator.FunctionalScoreQueryGenerator(randomQueries);
                    break;
                default:
                    throw new IllegalArgumentException("Unknwon query param " + param);
            }
            logger.debug("Using sub query generator: " + queryGenerator.getClass() + " , arity = " + arity);
            params.put(ARITY, arity.toString());
            queryGenerator.setUp(params, randomQueries);
            generators.add(queryGenerator);
        }
    }

    @Override
    public String getQueryId() {
        return query;
    }

    @Override
    public Query generateQuery(Query query) {
        for (QueryGenerator generator : generators) {
            generator.generateQuery(query);
        }
        appendbaseQuery(randomQueries, query);
        appendRandomSessionId(randomQueries.getSessionIds(), query);
        query.putAll(baseQueriesFromCLI);
        return query;
    }
}

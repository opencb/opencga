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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.Score;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.*;

/**
 * Created by jtarraga on 07/04/17.
 *
 * @author Joaquin Tarraga &lt;joaquintarraga@gmail.com&gt;
 */
public abstract class ScoreQueryGenerator extends ConfiguredQueryGenerator {
    protected static final List<String> DEF_OPS = Arrays.asList(">", "<");
    private List<Score> scores;
    private String queryKey;
    private Logger logger = LogManager.getLogger(getClass());

    public ScoreQueryGenerator(String queryKey) {
        this(new ArrayList<>(), queryKey);
    }

    public ScoreQueryGenerator(List<Score> scores, String queryKey) {
        super();
        this.scores = scores;
        this.queryKey = queryKey;
    }

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
        if (getArity() > scores.size()) {
            logger.warn("Only " + scores.size() + " scores available for " + getClass() + ". Adjusting arity.");
            setArity(scores.size());
        }
    }

    @Override
    public Query generateQuery(Query query) {
        if (getArity() != scores.size()) {
            Collections.shuffle(scores, random);
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < getArity(); i++) {
            Score score = scores.get(i);
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(score.getId());
            sb.append(getOperators(score));
            sb.append(random.nextDouble() * (score.getMax() - score.getMin()) + score.getMin());
        }
        query.append(queryKey, sb.toString());

        return query;
    }

    private String getOperators(Score score) {
        return (Objects.isNull(score.getOperators()))
                ? DEF_OPS.get(random.nextInt(2)) : score.getOperators().get(random.nextInt(score.getOperators().size()));
    }


    public static class ProteinSubstQueryGenerator extends ScoreQueryGenerator {

        public ProteinSubstQueryGenerator(RandomQueries randomQueries) {
            super(randomQueries.getProteinSubstitution(), VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

    public static class ConservationQueryGenerator extends ScoreQueryGenerator {

        public ConservationQueryGenerator(RandomQueries randomQueries) {
            super(Arrays.asList(randomQueries.getConservation()), VariantQueryParam.ANNOT_CONSERVATION.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

    public static class FunctionalScoreQueryGenerator extends ScoreQueryGenerator {

        public FunctionalScoreQueryGenerator(RandomQueries randomQueries) {
            super(randomQueries.getFunctionalScore(), VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

    public static class PopulationFrequenciesRefQueryGenerator extends ScoreQueryGenerator {

        public PopulationFrequenciesRefQueryGenerator(RandomQueries randomQueries) {
            super(randomQueries.getPopulationFrequencies(), VariantQueryParam.ANNOT_POPULATION_REFERENCE_FREQUENCY.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

    public static class PopulationFrequenciesMafQueryGenerator extends ScoreQueryGenerator {

        public PopulationFrequenciesMafQueryGenerator(RandomQueries randomQueries) {
            super(randomQueries.getPopulationFrequencies(), VariantQueryParam.ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

    public static class PopulationFrequenciesAltQueryGenerator extends ScoreQueryGenerator {

        public PopulationFrequenciesAltQueryGenerator(RandomQueries randomQueries) {
            super(randomQueries.getPopulationFrequencies(), VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

    public static class QualQueryGenerator extends ScoreQueryGenerator {

        public QualQueryGenerator(RandomQueries randomQueries) {
            super(Arrays.asList(randomQueries.getQual()), VariantQueryParam.QUAL.key());
        }

        @Override
        public void setUp(Map<String, String> params, RandomQueries queries) {
            super.setUp(params);
        }
    }

}

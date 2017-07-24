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

import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jtarraga on 07/04/17.
 *
 * @author Joaquín Tárraga &lt;joaquintarraga@gmail.com&gt;
 */
public abstract class ScoreQueryGenerator extends QueryGenerator {
    protected static final List<String> NUM_OPS = Arrays.asList(">", ">=", "<", "<=");
    private ArrayList<Score> scores;
    private List<String> ops;
    private String queryKey;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public ScoreQueryGenerator(List<String> ops, String queryKey) {
        this(new ArrayList<>(), ops, queryKey);
    }

    public ScoreQueryGenerator(ArrayList<Score> scores, List<String> ops, String queryKey) {
        super();
        this.scores = scores;
        this.ops = ops;
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
            sb.append(score.getName());
            sb.append(ops.get(random.nextInt(ops.size())));
            sb.append(random.nextDouble() * (score.getMax() - score.getMin()) + score.getMin());
        }
        query.append(queryKey, sb.toString());

        return query;
    }

    protected ScoreQueryGenerator addScore(Score score) {
        if (scores == null) {
            scores = new ArrayList<>();
        }
        scores.add(score);
        return this;
    }

    public static class Score {
        private String name;
        private double min;
        private double max;

        public Score(String name, double min, double max) {
            this.name = name;
            this.min = min;
            this.max = max;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public double getMin() {
            return min;
        }

        public void setMin(double min) {
            this.min = min;
        }

        public double getMax() {
            return max;
        }

        public void setMax(double max) {
            this.max = max;
        }
    }

    public static class ProteinSubstQueryGenerator extends ScoreQueryGenerator {

        public ProteinSubstQueryGenerator() {
            super(NUM_OPS, VariantQueryParam.ANNOT_PROTEIN_SUBSTITUTION.key());
            addScore(new Score("sift", 0, 1));
            addScore(new Score("polyphen", 0, 1));
        }
    }

    public static class ConservationQueryGenerator extends ScoreQueryGenerator {

        public ConservationQueryGenerator() {
            super(NUM_OPS, VariantQueryParam.ANNOT_CONSERVATION.key());
            addScore(new Score("phylop", 0, 1));
            addScore(new Score("phastCons", 0, 1));
            addScore(new Score("gerp", 0, 1));
        }
    }

    public static class FunctionalScoreQueryGenerator extends ScoreQueryGenerator {

        public FunctionalScoreQueryGenerator() {
            super(NUM_OPS, VariantQueryParam.ANNOT_FUNCTIONAL_SCORE.key());
            addScore(new Score("cadd_raw", 0, 1));
            addScore(new Score("cadd_scaled", -10, 40));
        }
    }
}

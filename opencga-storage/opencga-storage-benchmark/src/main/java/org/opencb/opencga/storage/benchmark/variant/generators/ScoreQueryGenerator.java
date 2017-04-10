package org.opencb.opencga.storage.benchmark.variant.generators;

import org.opencb.commons.datastore.core.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by jtarraga on 07/04/17.
 *
 * @author Joaquín Tárraga &lt;joaquintarraga@gmail.com&gt;
 */
public class ScoreQueryGenerator extends QueryGenerator {
    protected List<Score> scores;
    private List<String> ops;
    private String queryKey;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public ScoreQueryGenerator(List<Score> scores, List<String> ops, String queryKey) {
        super();
        this.scores = scores;
        this.ops = ops;
        this.queryKey = queryKey;
    }

    @Override
    public void setUp(Map<String, String> params) {
        super.setUp(params);
    }

    @Override
    public Query generateQuery() {
        Query query = new Query();

        Score score = scores.get(random.nextInt(scores.size()));

        StringBuilder sb = new StringBuilder();
        sb.append(score.getName());
        sb.append(ops.get(random.nextInt(ops.size())));
        sb.append(random.nextDouble() * (score.getMax() - score.getMin()) + score.getMin());

        query.append(queryKey, sb.toString());
        return query;
    }

    public class Score {
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
}

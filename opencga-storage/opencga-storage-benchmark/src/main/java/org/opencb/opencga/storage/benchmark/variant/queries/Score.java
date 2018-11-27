package org.opencb.opencga.storage.benchmark.variant.queries;

import java.util.List;

/**
 * Created by wasim on 01/11/18.
 */
public class Score {
    private String id;
    private double min;
    private double max;
    private List<String> operators;

    public Score() {
    }

    public Score(String id, double min, double max, List<String> operators) {
        this.id = id;
        this.min = min;
        this.max = max;
        this.operators = operators;
    }

    public Score(String id, double min, double max) {
        this.id = id;
        this.min = min;
        this.max = max;
    }

    public String getId() {
        return id;
    }

    public Score setId(String id) {
        this.id = id;
        return this;
    }

    public double getMin() {
        return min;
    }

    public Score setMin(double min) {
        this.min = min;
        return this;
    }

    public double getMax() {
        return max;
    }

    public Score setMax(double max) {
        this.max = max;
        return this;
    }

    public List<String> getOperators() {
        return operators;
    }

    public Score setOperators(List<String> operators) {
        this.operators = operators;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Score{");
        sb.append("id='").append(id).append('\'');
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", operators=").append(operators);
        sb.append('}');
        return sb.toString();
    }
}

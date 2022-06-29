package org.opencb.opencga.core.models.job;

import java.util.Date;
import java.util.List;

public class ExecutionTop {
    private Date date;
    private ExecutionTopStats stats;
    private List<Execution> executions;

    public ExecutionTop() {
    }

    public ExecutionTop(Date date, ExecutionTopStats stats, List<Execution> executions) {
        this.date = date;
        this.stats = stats;
        this.executions = executions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ExecutionTop{");
        sb.append("date=").append(date);
        sb.append(", stats=").append(stats);
        sb.append(", executions=").append(executions);
        sb.append('}');
        return sb.toString();
    }

    public Date getDate() {
        return date;
    }

    public ExecutionTop setDate(Date date) {
        this.date = date;
        return this;
    }

    public ExecutionTopStats getStats() {
        return stats;
    }

    public ExecutionTop setStats(ExecutionTopStats stats) {
        this.stats = stats;
        return this;
    }

    public List<Execution> getExecutions() {
        return executions;
    }

    public ExecutionTop setExecutions(List<Execution> executions) {
        this.executions = executions;
        return this;
    }
}

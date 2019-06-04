package org.opencb.opencga.core.config;

/**
 * Created by wasim on 13/02/19.
 */
public class HealthCheck {
    private int interval;

    public int getInterval() {
        return interval;
    }

    public HealthCheck setInterval(int interval) {
        this.interval = interval;
        return this;
    }

    public HealthCheck() {
        interval = 30;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HealthCheck{");
        sb.append("interval=").append(interval);
        sb.append('}');
        return sb.toString();
    }
}

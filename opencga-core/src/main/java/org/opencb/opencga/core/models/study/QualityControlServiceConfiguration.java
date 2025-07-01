package org.opencb.opencga.core.models.study;

public class QualityControlServiceConfiguration extends CatalogServiceConfiguration {

    private Policy policy;

    public QualityControlServiceConfiguration() {
    }

    public QualityControlServiceConfiguration(boolean active, int concurrentJobs, Policy policy) {
        super(active, concurrentJobs);
        this.policy = policy;
    }

    public static QualityControlServiceConfiguration defaultConfiguration() {
        return new QualityControlServiceConfiguration(false, 5, Policy.NIGHTLY);
    }

    public enum Policy {
        IMMEDIATELY,
        NIGHTLY,
        WEEKLY
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QualityControlServiceConfiguration{");
        sb.append("policy=").append(policy);
        sb.append('}');
        return sb.toString();
    }

    public Policy getPolicy() {
        return policy;
    }

    public QualityControlServiceConfiguration setPolicy(Policy policy) {
        this.policy = policy;
        return this;
    }

    @Override
    public QualityControlServiceConfiguration setActive(boolean active) {
        super.setActive(active);
        return this;
    }

    @Override
    public QualityControlServiceConfiguration setConcurrentJobs(int concurrentJobs) {
        super.setConcurrentJobs(concurrentJobs);
        return this;
    }
}

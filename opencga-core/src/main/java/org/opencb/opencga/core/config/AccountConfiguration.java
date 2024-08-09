package org.opencb.opencga.core.config;

public class AccountConfiguration {

    private int maxLoginAttempts;
    private int passwordExpirationDays;

    public AccountConfiguration() {
    }

    public AccountConfiguration(int maxLoginAttempts, int passwordExpirationDays) {
        this.maxLoginAttempts = maxLoginAttempts;
        this.passwordExpirationDays = passwordExpirationDays;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AccountConfiguration{");
        sb.append("maxLoginAttempts=").append(maxLoginAttempts);
        sb.append(", passwordExpirationDays=").append(passwordExpirationDays);
        sb.append('}');
        return sb.toString();
    }

    public int getMaxLoginAttempts() {
        return maxLoginAttempts;
    }

    public AccountConfiguration setMaxLoginAttempts(int maxLoginAttempts) {
        this.maxLoginAttempts = maxLoginAttempts;
        return this;
    }

    public int getPasswordExpirationDays() {
        return passwordExpirationDays;
    }

    public AccountConfiguration setPasswordExpirationDays(int passwordExpirationDays) {
        this.passwordExpirationDays = passwordExpirationDays;
        return this;
    }
}

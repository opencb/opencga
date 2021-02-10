package org.opencb.opencga.core.config;

public class Optimizations {

    /**
     * Boolean indicating whether permission checks can be simplified. They could be simplified if owners do not remove permissions at
     * any entity level other than Study. By default, false.
     */
    private boolean simplifyPermissions;

    public Optimizations() {
    }

    public Optimizations(boolean simplifyPermissions) {
        this.simplifyPermissions = simplifyPermissions;
    }

    public boolean isSimplifyPermissions() {
        return simplifyPermissions;
    }

    public Optimizations setSimplifyPermissions(boolean simplifyPermissions) {
        this.simplifyPermissions = simplifyPermissions;
        return this;
    }

}

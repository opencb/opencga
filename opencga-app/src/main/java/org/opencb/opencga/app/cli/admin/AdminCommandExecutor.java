package org.opencb.opencga.app.cli.admin;

import org.opencb.opencga.app.cli.CommandExecutor;

/**
 * Created on 03/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class AdminCommandExecutor extends CommandExecutor {

    protected String adminPassword;


    public AdminCommandExecutor(AdminCliOptionsParser.AdminCommonCommandOptions options) {
        super(options);
    }

    protected void init(AdminCliOptionsParser.AdminCommonCommandOptions options) {
        super.init(options);
        this.adminPassword = options.adminPassword;
    }

    public String getAdminPassword() {
        return adminPassword;
    }

    public CommandExecutor setAdminPassword(String adminPassword) {
        this.adminPassword = adminPassword;
        return this;
    }
}

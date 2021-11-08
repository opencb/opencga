package org.opencb.opencga.app.cli.main;

import org.opencb.opencga.app.cli.CliSession;
import org.opencb.opencga.core.common.TimeUtils;

import java.util.Date;

public class Session {

    private OpencgaCliShellExecutor shell;

    public OpencgaCliShellExecutor getShell() {
        return shell;
    }

    public Session setShell(OpencgaCliShellExecutor shell) {
        this.shell = shell;
        return this;
    }

    public boolean isShell() {
        return shell != null;
    }

    public boolean isValidSession() {
        if (Long.parseLong(TimeUtils.getTime(new Date())) > Long.parseLong(CliSession.getInstance().getExpirationTime())) {
            return false;
        }
        return true;
    }
}

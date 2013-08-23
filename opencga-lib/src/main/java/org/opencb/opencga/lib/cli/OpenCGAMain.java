package org.opencb.opencga.lib.cli;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.opencb.opencga.common.Config;
import org.opencb.opencga.common.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class OpenCGAMain {

    public static Tomcat tomcat;
    public static Properties properties;

    public static void main(String[] args) {

        String home = args[0];
        if (home == null || home.trim().equals("")) {
            home = ".";
        }

        properties = Config.getLocalServerProperties(home);
        int port = Integer.parseInt(properties.getProperty("OPENCGA.LOCAL.PORT", "61976"));

        tomcat = new Tomcat();
        tomcat.setPort(port);

        Context ctx = tomcat.addContext("/opencga/rest", new File(".").getAbsolutePath());

        Tomcat.addServlet(ctx, "fetch", new FetchServlet());
        ctx.addServletMapping("/storage/fetch", "fetch");

        Tomcat.addServlet(ctx, "admin", new AdminServlet());
        ctx.addServletMapping("/admin", "admin");

        Tomcat.addServlet(ctx, "getdirs", new GetFoldersServlet());
        ctx.addServletMapping("/getdirs", "getdirs");

        try {
            tomcat.start();
            tomcat.getServer().await();
        } catch (LifecycleException e) {
            e.printStackTrace();
        }
    }

    public static void stop() throws LifecycleException {
        tomcat.stop();
        // FIXME
        Path tomcatTempPath = Paths.get("tomcat." + properties.getProperty("OPENCGA.LOCAL.PORT", "61976"));
        System.out.println(tomcatTempPath.toAbsolutePath().toString());
        if (Files.exists(tomcatTempPath)) {
            try {
                IOUtils.deleteDirectory(tomcatTempPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

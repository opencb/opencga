/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.app.localserver;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.Tomcat;
import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.core.common.IOUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class LocalServer {

    public static Tomcat tomcat;
    public static Properties properties;

    public void launch(String[] args) {

        String home = args[0];
        if (home == null || home.trim().equals("")) {
            home = ".";
        }

        properties = Config.getStorageProperties(home);
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

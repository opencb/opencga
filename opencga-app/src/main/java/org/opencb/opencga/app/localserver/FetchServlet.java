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

import org.opencb.opencga.core.common.StringUtils;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FetchServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setHeader("Access-Control-Allow-Origin", "*");
        System.out.println("OpenCGA Local server");

        PrintWriter pw = resp.getWriter();

        String filePathStr = req.getParameter("filepath");//.replace(":", "/");
        String region = req.getParameter("region");
        if (filePathStr != null && region != null) {
            int dotPosition = filePathStr.lastIndexOf(".");
            String ext = filePathStr.substring(dotPosition + 1, filePathStr.length());
            Path filePath = Paths.get("/").resolve(StringUtils.parseObjectId(filePathStr));

            System.out.println("filepath: " + filePath + ", exists: " + Files.exists(filePath));
            System.out.println("region: " + region);
            System.out.println("extension: " + ext);

            // Convert String[] params to List<String>
            Map<String, String[]> map = req.getParameterMap();
            Map<String, List<String>> params = new HashMap<>();
            String key;
            List<String> value;
            Iterator<String> iterator = map.keySet().iterator();
            while (iterator.hasNext()) {
                key = iterator.next();
                value = Arrays.asList(map.get(key));
                params.put(key, value);
            }

            switch (ext) {
//                case "bam":
//                    BamManager bamManager = new BamManager();
//                    pw.write(bamManager.getByRegion(filePath, region, params));
//                    break;
//                case "vcf":
//                    VcfManager vcfManager = new VcfManager();
//                    pw.write(vcfManager.getByRegion(filePath, region, params));
//                    break;
                default:
                    pw.write("Unknown extension: " + ext);
                    break;
            }
        } else {
            pw.write("The following params are required: filepath, region.");
        }

        pw.close();
    }
}

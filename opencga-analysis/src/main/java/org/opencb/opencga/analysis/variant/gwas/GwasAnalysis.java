/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.analysis.variant.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.gwas.Gwas;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.core.annotations.Analysis;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

@Analysis(id = "GWAS", data = Analysis.AnalysisData.CLINICAL)
public class GwasAnalysis extends OpenCgaAnalysis {

    private Gwas gwas;
    private Query query1;
    private Query query2;

    private String token;

    public GwasAnalysis(String study, String opencgaHome, String sessionId) {
        super(study, opencgaHome, sessionId);
    }

    public GwasAnalysis(String study, List<String> list1, List<String> list2, ObjectMap executorParams, GwasConfiguration gwasConfiguration, String token) {
        super(study, "", token);
        gwas = new Gwas(executorParams, outDir, gwasConfiguration).setSampleList1(list1).setSampleList2(list2);
    }

    public GwasAnalysis(String study, String cohort1, String cohort2, ObjectMap executorParams, GwasConfiguration gwasConfiguration, String token) {
        super(study, "", token);
        gwas = new Gwas(executorParams, outDir, gwasConfiguration).setCohort1(cohort1).setCohort2(cohort2);
    }

    public GwasAnalysis(String study, Query query1, Query query2, ObjectMap executorParams, GwasConfiguration gwasConfiguration, String token) {
        super(study, "", token);
        this.query1 = query1;
        this.query2 = query2;
        gwas = new Gwas(executorParams, outDir, gwasConfiguration);
    }

    protected void checkToken(String token) {

    }

    @Override
    protected void exec() throws AnalysisException {
        checkToken(token);

        List<String> list1 = null;
        if (query1 != null) {
            // Get list1 from query1
            list1 = new ArrayList<>();
        }
        List<String> list2 = null;
        if (query2 != null) {
            // Get list2 from query2
            list2 = new ArrayList<>();
        }

        if (CollectionUtils.isNotEmpty(list1) || CollectionUtils.isNotEmpty(list2)) {
            gwas.setSampleList1(list1).setSampleList2(list2);
        }

        //ObjectMap executorParams = new ObjectMap("ID", "MongoIter");
        gwas.execute();
    }


    /**
     * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
     *
     * @param packageName The base package
     * @return The classes
     * @throws ClassNotFoundException aaa
     * @throws IOException aaa
     */
    public static Class[] getClasses(String packageName) throws ClassNotFoundException, IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
//        assert classLoader != null;
        String path = packageName.replace('.', '/');
        Enumeration<URL> resources = classLoader.getResources(path);
        List<File> dirs = new ArrayList<>();
        while (resources.hasMoreElements()) {

            URL resource = resources.nextElement();

//            URLConnection connection = resource.openConnection();
//            if (connection instanceof JarURLConnection) {
//                System.out.println("flipo = " + ((JarURLConnection) connection).getEntryName());
//            }


            dirs.add(new File(resource.getFile()));
        }
        ArrayList<Class> classes = new ArrayList<>();
        for (File directory : dirs) {
            classes.addAll(findClasses(directory, packageName));
        }
        return classes.toArray(new Class[classes.size()]);
    }

    /**
     * Recursive method used to find all classes in a given directory and subdirs.
     *
     * @param directory   The base directory
     * @param packageName The package name for classes found inside the base directory
     * @return The classes
     * @throws ClassNotFoundException
     */
    private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
        System.out.println("directory = " + directory);
        List<Class> classes = new ArrayList<>();
        if (!directory.exists()) {
            return classes;
        }
        File[] files = directory.listFiles();
        for (File file : files) {

            System.out.println("file = " + file);
            if (file.isDirectory()) {
                assert !file.getName().contains(".");
                classes.addAll(findClasses(file, packageName + "." + file.getName()));
            } else if (file.getName().endsWith(".class")) {
                classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
            }
        }
        return classes;
    }

}

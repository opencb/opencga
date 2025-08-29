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

package org.opencb.opencga.storage.benchmark;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.gui.ArgumentsPanel;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.control.gui.LoopControlPanel;
import org.apache.jmeter.control.gui.TestPlanGui;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.samplers.Sampler;
import org.apache.jmeter.save.SaveService;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.threads.gui.ThreadGroupGui;
import org.apache.jmeter.timers.ConstantTimer;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;
import org.apache.jorphan.collections.ListedHashTree;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class BenchmarkRunner {

    public enum ConnectionType {
        REST,
        DIRECT,
        GRPC,
    }

    public enum ExecutionMode {
        FIXED,
        RANDOM,
    }

    protected final StorageConfiguration storageConfiguration;
    protected final String storageEngine;
    protected Path jmeterHome;
    protected HashTree testPlanTree;
    protected final String dbName;
    protected final Path outdir;
    protected TestPlan testPlan;
    protected String resultFile;

    public BenchmarkRunner(StorageConfiguration storageConfiguration, Path jmeterHome, Path outdir) throws IOException {
        StorageEngineFactory.configure(storageConfiguration);
        this.storageConfiguration = storageConfiguration;

        storageEngine = storageConfiguration.getVariant().getDefaultEngine();
        dbName = storageConfiguration.getBenchmark().getDatabaseName();
        this.jmeterHome = jmeterHome;
        this.outdir = outdir;

        init();
    }

    private void init() throws IOException {
        File jmeterProperties = jmeterHome.resolve("bin").resolve("jmeter.properties").toFile();
        if (!jmeterProperties.exists()) {
            throw new FileNotFoundException("Could not find " + jmeterProperties.toString());
        }
        //JMeter Engine

        //JMeter initialization (properties, log levels, locale, etc)
        JMeterUtils.setJMeterHome(jmeterHome.toString());
        JMeterUtils.loadJMeterProperties(jmeterProperties.getPath());
        JMeterUtils.initLogging(); // you can comment this line out to see extra log messages of i.e. DEBUG level
        JMeterUtils.initLocale();

        // JMeter Test Plan, basically JOrphan HashTree
        testPlanTree = new ListedHashTree();

        // Test Plan
        testPlan = new TestPlan("Create JMeter Script From Java Code");
        testPlan.setProperty(TestElement.TEST_CLASS, TestPlan.class.getName());
        testPlan.setProperty(TestElement.GUI_CLASS, TestPlanGui.class.getName());
        testPlan.setUserDefinedVariables((Arguments) new ArgumentsPanel().createTestElement());


        //add Summarizer output to get test progress in stdout like:
        // summary =      2 in   1.3s =    1.5/s Avg:   631 Min:   290 Max:   973 Err:     0 (0.00%)
        Summariser summer = null;
        String summariserName = JMeterUtils.getPropDefault("summariser.name", "summary");
        if (!summariserName.isEmpty()) {
            summer = new Summariser(summariserName);
        }

        // Store execution results into a .jtl file
        resultFile = outdir.resolve(buildOutputFileName()) + ".jtl";
        ResultCollector resultCollector = new ResultCollector(summer);
        resultCollector.setFilename(resultFile);
        testPlanTree.add(testPlan, resultCollector);


        // Add test plan to Tree
        testPlanTree.add(testPlan);
    }

    public void addThreadGroup(List<? extends Sampler> samplers) {
        // Loop Controller
        LoopController loopController = new LoopController();
        loopController.setLoops(storageConfiguration.getBenchmark().getNumRepetitions());
        loopController.setFirst(true);
        loopController.setProperty(TestElement.TEST_CLASS, LoopController.class.getName());
        loopController.setProperty(TestElement.GUI_CLASS, LoopControlPanel.class.getName());
        loopController.initialize();

        // Thread Group
        ThreadGroup threadGroup = new ThreadGroup();
        threadGroup.setName("Thread-Group");
        threadGroup.setNumThreads(storageConfiguration.getBenchmark().getConcurrency());
        threadGroup.setRampUp(1);
        threadGroup.setSamplerController(loopController);
        threadGroup.setProperty(TestElement.TEST_CLASS, ThreadGroup.class.getName());
        threadGroup.setProperty(TestElement.GUI_CLASS, ThreadGroupGui.class.getName());

        // Construct Test Plan from previously initialized elements
        HashTree threadGroupHashTree = testPlanTree.add(testPlan, threadGroup);

        ConstantTimer timer = new ConstantTimer();
        timer.setDelay(String.valueOf(storageConfiguration.getBenchmark().getDelay()));
        timer.setName("timer");

        // Add samplers to the ThreadGroup
        threadGroupHashTree.add(samplers);
        // Add timer to the ThreadGroup
        threadGroupHashTree.add(timer);
    }

    public void run() throws IOException {

        Files.createDirectories(outdir);
        // save generated test plan to JMeter's .jmx file format
        File jmxFile = outdir.resolve(buildOutputFileName() + ".jmx").toFile();
        SaveService.saveTree(testPlanTree, new FileOutputStream(jmxFile));

        StandardJMeterEngine jmeter;
        jmeter = new StandardJMeterEngine();
        // Run Test Plan
        jmeter.configure(testPlanTree);
        jmeter.run();

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(stream)) {
            printResults(jmxFile, out);
        }

        System.out.println(stream.toString());
        try (FileOutputStream fileOutputStream = new FileOutputStream(outdir.resolve("results.txt").toFile())) {
            stream.writeTo(fileOutputStream);
        }
    }

    private String buildOutputFileName() {
        return dbName.replaceAll("[@:]", "_")
                + "." + "benchmark"
                + "." + storageConfiguration.getBenchmark().getMode();
    }

    private static final class QueryResultStats {
        private String queryId;
        private int count;
        private int successCount;
        private double totalTime;
        private double bytes;

        private QueryResultStats(String queryId) {
            this.queryId = queryId;
        }
    }

    private void printResults(File jmxFile, PrintStream out) {
        Map<String, QueryResultStats> results = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(resultFile))) {
            String line = br.readLine(); // ignore first line
            while ((line = br.readLine()) != null) {
                String[] splittedResult = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                String label = splittedResult[2];
                QueryResultStats result = results.computeIfAbsent(label, QueryResultStats::new);
                result.count++;
                // elapsed
                result.totalTime += Double.parseDouble(splittedResult[1]);
                result.bytes += Long.parseLong(splittedResult[9]);
                if (splittedResult[7].equals("true")) {
                    result.successCount++;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        out.println();
        out.println();
        out.println("*********   Test completed   **********");
        out.println();
        out.println("Concurrency (number of Threads) : " + storageConfiguration.getBenchmark().getConcurrency());
        out.println("Repetitions : " + storageConfiguration.getBenchmark().getNumRepetitions());
        out.println("Delay (ms) : " + storageConfiguration.getBenchmark().getDelay());
        out.println("Connection Type : " + storageConfiguration.getBenchmark().getConnectionType());
        if (storageConfiguration.getBenchmark().getConnectionType().equals(ConnectionType.REST.name())) {
            out.println("URL : " + storageConfiguration.getBenchmark().getRest());
        }
        out.println();
        int keyLength = Math.max(18, results.keySet().stream().mapToInt(String::length).max().orElse(0));
        String format = "| %3s | %-" + keyLength + "s | %12s | %18s | %18s | %18s |\n";
        out.printf(format,   "#",           "Query ID",  "Num queries",     "Avg. Time (ms)",      "Avg. size (B)",  "Success Ratio (%)");
        out.printf(format, "---", StringUtils.repeat('-', keyLength), "------------",
                "------------------", "------------------", "------------------");
        int i = 0;
        for (QueryResultStats result : results.values()) {
            String key = result.queryId;
            if (key.startsWith("\"") && key.endsWith("\"")) {
                key = key.substring(1, key.length() - 1);
            }
            double avgTime = result.totalTime / result.count;
            double successRatio = (double) result.successCount / result.count * 100;
            double avgSize = result.bytes / result.count;
            out.printf(format, ++i, key,
                    result.count,
                    String.format("%.2f", avgTime),
                    String.format("%.2f", avgSize),
                    String.format("%.2f", successRatio));
        }

        out.println();
        out.println();
        out.println("Test Results File  : " + resultFile);
        out.println("JMeter Script File : " + jmxFile.toPath());
        out.println();
        out.println();
        out.println("** How To Generate JMeter HTML Report **");
        out.println();
        out.println();
        out.println("Use the following command from outDir (" + outdir + ") :");
        out.println();
        out.println("cd " + outdir + "");
        out.println("jmeter -g " + buildOutputFileName() + ".jtl -o Dashboard");
        out.println();
        out.println("** How To Generate MatPlotLib Charts **");
        out.println("python3 " + jmeterHome.getParent().resolve("bin").resolve("benchmark_plot_series.py") + " " + outdir.getParent());
        out.println();

    }
}

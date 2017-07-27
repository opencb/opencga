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
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

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

        storageEngine = storageConfiguration.getBenchmark().getStorageEngine();
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
        testPlanTree = new HashTree();

        // Test Plan
        testPlan = new TestPlan("Create JMeter Script From Java Code");
        testPlan.setProperty(TestElement.TEST_CLASS, TestPlan.class.getName());
        testPlan.setProperty(TestElement.GUI_CLASS, TestPlanGui.class.getName());
        testPlan.setUserDefinedVariables((Arguments) new ArgumentsPanel().createTestElement());


        //add Summarizer output to get test progress in stdout like:
        // summary =      2 in   1.3s =    1.5/s Avg:   631 Min:   290 Max:   973 Err:     0 (0.00%)
        Summariser summer = null;
        String summariserName = JMeterUtils.getPropDefault("summariser.name", "summary");
        if (summariserName.length() > 0) {
            summer = new Summariser(summariserName);
        }

        // Store execution results into a .jtl file
        resultFile = outdir.resolve(buildOutputFileName() + ".jtl").toString();
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

        // Add samplers to the ThreadGroup
        threadGroupHashTree.add(samplers);
    }

    public void run() throws IOException {

        // save generated test plan to JMeter's .jmx file format
        File jmxFile = outdir.resolve(buildOutputFileName() + ".jmx").toFile();
        SaveService.saveTree(testPlanTree, new FileOutputStream(jmxFile));

        StandardJMeterEngine jmeter;
        jmeter = new StandardJMeterEngine();
        // Run Test Plan
        jmeter.configure(testPlanTree);
        jmeter.run();

        System.out.println("Test completed. See " + resultFile + " file for results");
        System.out.println("JMeter .jmx script is available at " + jmxFile.toPath());
    }

    private String buildOutputFileName() {
        return dbName + "." + "benchmark";
    }

}

package org.opencb.opencga.storage.benchmark;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.gui.ArgumentsPanel;
import org.apache.jmeter.control.LoopController;
import org.apache.jmeter.control.gui.LoopControlPanel;
import org.apache.jmeter.control.gui.TestPlanGui;
import org.apache.jmeter.engine.StandardJMeterEngine;
import org.apache.jmeter.reporters.ResultCollector;
import org.apache.jmeter.reporters.Summariser;
import org.apache.jmeter.save.SaveService;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestPlan;
import org.apache.jmeter.threads.ThreadGroup;
import org.apache.jmeter.threads.gui.ThreadGroupGui;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.collections.HashTree;
import org.opencb.opencga.storage.benchmark.variant.generators.GeneQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.generators.RegionQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineRestSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineSampler;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;


public class Main {

    public static void main(String[] argv) throws Exception {

//        File jmeterHome = new File(System.getProperty("jmeter.home"));
        File jmeterHome = new File("/home/hpccoll1/opt/jmeter/");
        String dataDir = "/home/hpccoll1/appl/opencga/jmeter";
        String applDir = "/home/hpccoll1/opt/opencga_platinum";
        Path outdirPath = Paths.get("").toAbsolutePath();

        StorageConfiguration storageConfiguration;

        try (FileInputStream is = new FileInputStream(Paths.get(applDir, "conf", "storage-configuration.yml").toFile())) {
            storageConfiguration = StorageConfiguration.load(is);
            StorageEngineFactory.configure(storageConfiguration);
        }

        if (!jmeterHome.exists()) {
            System.err.println("jmeter.home property is not set or pointing to incorrect location");
            System.exit(1);
        }
        File jmeterProperties = Paths.get(jmeterHome.getPath(), "bin", "jmeter.properties").toFile();
        if (jmeterProperties.exists()) {
            //JMeter Engine
            StandardJMeterEngine jmeter = new StandardJMeterEngine();

            //JMeter initialization (properties, log levels, locale, etc)
            JMeterUtils.setJMeterHome(jmeterHome.getPath());
            JMeterUtils.loadJMeterProperties(jmeterProperties.getPath());
            JMeterUtils.initLogging(); // you can comment this line out to see extra log messages of i.e. DEBUG level
            JMeterUtils.initLocale();

            // JMeter Test Plan, basically JOrphan HashTree
            HashTree testPlanTree = new HashTree();


//            VariantStorageEngineSampler variantStorageSampler1 = new VariantStorageEngineDirectSampler();
            VariantStorageEngineSampler variantStorageSampler1 = new VariantStorageEngineRestSampler("localhost", 9090);
            variantStorageSampler1.setStorageEngine("mongodb");
            variantStorageSampler1.setDBName("opencga_jmeter_platinum_platinum");
            variantStorageSampler1.setQueryGenerator(GeneQueryGenerator.class);
            variantStorageSampler1.setDataDir(dataDir);

            VariantStorageEngineSampler variantStorageSampler2 = new VariantStorageEngineRestSampler("localhost", 9090);
            variantStorageSampler2.setStorageEngine("mongodb");
            variantStorageSampler2.setDBName("opencga_jmeter_platinum_platinum");
            variantStorageSampler2.setQueryGenerator(RegionQueryGenerator.class);
            variantStorageSampler2.setDataDir(dataDir);

            // Loop Controller
            LoopController loopController = new LoopController();
            loopController.setLoops(20);
            loopController.setFirst(true);
            loopController.setProperty(TestElement.TEST_CLASS, LoopController.class.getName());
            loopController.setProperty(TestElement.GUI_CLASS, LoopControlPanel.class.getName());
            loopController.initialize();

            // Thread Group
            ThreadGroup threadGroup = new ThreadGroup();
            threadGroup.setName("Thread-Group");
            threadGroup.setNumThreads(5);
            threadGroup.setRampUp(1);
            threadGroup.setSamplerController(loopController);
            threadGroup.setProperty(TestElement.TEST_CLASS, ThreadGroup.class.getName());
            threadGroup.setProperty(TestElement.GUI_CLASS, ThreadGroupGui.class.getName());

            // Test Plan
            TestPlan testPlan = new TestPlan("Create JMeter Script From Java Code");
            testPlan.setProperty(TestElement.TEST_CLASS, TestPlan.class.getName());
            testPlan.setProperty(TestElement.GUI_CLASS, TestPlanGui.class.getName());
            testPlan.setUserDefinedVariables((Arguments) new ArgumentsPanel().createTestElement());

            // Construct Test Plan from previously initialized elements
            testPlanTree.add(testPlan);
            HashTree threadGroupHashTree = testPlanTree.add(testPlan, threadGroup);
            threadGroupHashTree.add(variantStorageSampler1);
            threadGroupHashTree.add(variantStorageSampler2);
//            threadGroupHashTree.add(jUnitSampler);
//            threadGroupHashTree.add(javaSampler);
//                threadGroupHashTree.add(blazemetercomSampler);
//                threadGroupHashTree.add(examplecomSampler);

            // save generated test plan to JMeter's .jmx file format
            File jmxFile = outdirPath.resolve("example.jmx").toFile();
            SaveService.saveTree(testPlanTree, new FileOutputStream(jmxFile));

            //add Summarizer output to get test progress in stdout like:
            // summary =      2 in   1.3s =    1.5/s Avg:   631 Min:   290 Max:   973 Err:     0 (0.00%)
            Summariser summer = null;
            String summariserName = JMeterUtils.getPropDefault("summariser.name", "summary");
            if (summariserName.length() > 0) {
                summer = new Summariser(summariserName);
            }

            // Store execution results into a .jtl file
            String logFile = outdirPath.resolve("example.jtl").toString();
            ResultCollector logger = new ResultCollector(summer);
            logger.setFilename(logFile);
            testPlanTree.add(testPlanTree.getArray()[0], logger);

//            String reportFile = outdirPath.resolve("report").toString();
//            ReportGenerator reportGenerator = new ReportGenerator(reportFile, logger);
//            testPlanTree.add(testPlanTree.getArray()[0], reportGenerator);
//            reportGenerator.generate();

            // Run Test Plan
            jmeter.configure(testPlanTree);
            jmeter.run();

            System.out.println("Test completed. See " + logFile + " file for results");
            System.out.println("JMeter .jmx script is available at " + jmxFile.toPath());
            System.exit(0);

        }

    }
}

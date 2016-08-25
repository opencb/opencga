package org.opencb.opencga.catalog.monitor.executors;

import org.apache.tools.ant.types.Commandline;
import org.opencb.opencga.catalog.config.Execution;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.core.SgeManager;
import org.opencb.opencga.core.exec.Command;
import org.opencb.opencga.core.exec.SingleProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.catalog.models.Job.JobStatus.*;

/**
 * Created by pfurio on 24/08/16.
 */
public class SGEManager {

    protected static Logger logger = LoggerFactory.getLogger(SgeManager.class);
    private static final Map<String, String> STATE_DIC;
    private Execution execution;

    static {
        STATE_DIC = new HashMap<>();
        STATE_DIC.put("r", RUNNING);
        STATE_DIC.put("t", QUEUED);
        STATE_DIC.put("qw", QUEUED);
        STATE_DIC.put("Eqw", ERROR);
    }

    public SGEManager(Execution execution) {
        this.execution = execution;
    }


    public void queueJob(String toolName, String wumJobName, int wumUserId, String commandLine, ExecutorConfig config) throws Exception {
        queueJob(toolName, wumJobName, wumUserId, commandLine, getQueueName(toolName), config);
    }

    public void queueJob(String toolName, String wumJobName, int wumUserId, String commandLine, String queue, ExecutorConfig config)
            throws Exception {

        if (!Paths.get(config.getOutdir()).toFile().exists()) {
            logger.error("Output directory {} does not exist", config.getOutdir());
            throw new Exception("The output directory " + config.getOutdir() + " does not exist.");
        }

        queue = queue == null || queue.isEmpty() ? getQueueName(toolName) : queue;
        String outFile = config.getStdout();
        String errFile = config.getStderr();

        if (!Paths.get(outFile).getParent().toFile().exists() || !Paths.get(errFile).getParent().toFile().exists()) {
            logger.warn("Directory where the logger files would be created not found. Out: {}, Err: {}", outFile, errFile);
        }

        // init sge job
        String outScript = Paths.get(config.getOutdir(), "command_line.sh").toString();
        Files.write(Paths.get(outScript), commandLine.getBytes());

        ArrayList<String> args = new ArrayList<>(Arrays.asList(
                "qsub", "-V",
                "-N", getSgeJobName(toolName, wumJobName),
                "-o", outFile,
                "-e", errFile,
                "-q", queue,
                outScript));

        String[] cmdArray = args.toArray(new String[args.size()]);
        logger.info("SgeManager: Enqueuing job: " + Commandline.toString(cmdArray));

        // thrown command to shell
        Command sgeCommand = new Command(cmdArray, null);
        SingleProcess sp = new SingleProcess(sgeCommand);
        sp.getRunnableProcess().run();
        if (sgeCommand.getExitValue() != 0 || sgeCommand.getException() != null) {
            throw new Exception("Can't queue job " + getSgeJobName(toolName, wumJobName) + ". qsub returned " + sgeCommand.getExitValue()
                    + " and message:" + sgeCommand.getException());
        }
    }

    private String getSgeJobName(String toolName, String wumJobId) {
        return toolName.replace(" ", "_") + "_" + wumJobId;
    }

    private String getQueueName(String toolName) throws Exception {
        String defaultQueue = getDefaultQueue();
        logger.debug("SgeManager: default queue: " + defaultQueue);

        // get all available queues
        List<String> queueList = getQueueList();
        logger.debug("SgeManager: available queues: " + queueList);

        // search corresponding queue
        String selectedQueue = defaultQueue;
        // TODO: Check queue selection depending on the tool
//        String queueProperty;
//        for (String queue : queueList) {
//            if (!queue.equalsIgnoreCase(defaultQueue)) {
//                queueProperty = "OPENCGA.SGE." + queue.toUpperCase() + ".TOOLS";
//                if (analysisProperties.containsKey(queueProperty)) {
//                    if (belongsTheToolToQueue(analysisProperties.getProperty(queueProperty), toolName)) {
//                        selectedQueue = queue;
//                    }
//                }
//            }
//        }
        logger.info("SgeManager: selected queue for tool '" + toolName + "': " + selectedQueue);
        return selectedQueue;
    }

    private String getDefaultQueue() {
        return execution.getDefaultQueue();
    }

    private List<String> getQueueList() {
        return Arrays.asList(execution.getAvailableQueues().split(","));
    }

    public static String status(String jobId) throws Exception {
        String status = Job.JobStatus.UNKNOWN;

        String xml;
        try {
            Process p = Runtime.getRuntime().exec("qstat -xml");
            StringBuilder stdOut = new StringBuilder();
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String aux = "";
            while ((aux = br.readLine()) != null) {
                stdOut.append(aux);
            }
            xml = stdOut.toString();
            br.close();
        } catch (Exception e) {
            logger.error(e.toString());
            throw new Exception("ERROR: can't get status for job " + jobId + ".");
        }

        if (xml != null) {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(new InputSource(new StringReader(xml)));
                doc.getDocumentElement().normalize();
                NodeList nodeLst = doc.getElementsByTagName("job_list");

                for (int s = 0; s < nodeLst.getLength(); s++) {
                    Node fstNode = nodeLst.item(s);

                    if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element fstElmnt = (Element) fstNode;
                        NodeList fstNmElmntLst = fstElmnt.getElementsByTagName("JB_name");
                        Element fstNmElmnt = (Element) fstNmElmntLst.item(0);
                        NodeList fstNm = fstNmElmnt.getChildNodes();
                        String jobName = ((Node) fstNm.item(0)).getNodeValue();
                        if (jobName.contains(jobId)) {
                            NodeList lstNmElmntLst = fstElmnt.getElementsByTagName("state");
                            Element lstNmElmnt = (Element) lstNmElmntLst.item(0);
                            NodeList lstNm = lstNmElmnt.getChildNodes();
                            status = ((Node) lstNm.item(0)).getNodeValue();
                        }
                    }
                }
            } catch (Exception e) {
                logger.error(e.toString());
                throw new Exception("ERROR: can't get status for job " + jobId + ".");
            }
        }

        if (!status.equals(Job.JobStatus.UNKNOWN)) {
            status = STATE_DIC.get(status);
        } else {
            String command = "qacct -j *" + jobId + "*";
//            logger.info(command);
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            String exitStatus = null;
            String failed = null;
            while ((line = in.readLine()) != null) {
                if (line.contains("exit_status")) {
                    exitStatus = line.replace("exit_status", "").trim();
                }
                if (line.contains("failed")) {
                    failed = line.replace("failed", "").trim();
                }
            }
            p.waitFor();
            in.close();

            if (exitStatus != null && failed != null) {
                if ("0".equals(exitStatus)) {
                    status = DONE;
                } else {
                    status = ERROR;
                }
            }
        }
        return status;
    }
}

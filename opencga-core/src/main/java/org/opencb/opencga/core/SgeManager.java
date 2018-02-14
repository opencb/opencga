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

package org.opencb.opencga.core;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.exec.Command;
import org.opencb.commons.exec.SingleProcess;
import org.opencb.opencga.core.config.Configuration;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class  SgeManager {


    private static final Map<String, String> stateDic;
    public static final String UNKNOWN = "unknown";
    public static final String RUNNING = "running";
    public static final String TRANSFERRED = "transferred";
    public static final String QUEUED = "queued";
    public static final String ERROR = "error";
    public static final String FINISHED = "finished";
    public static final String EXECUTION_ERROR = "execution error";

    protected static Logger logger = LoggerFactory.getLogger(SgeManager.class);
//    @Deprecated
//    private static Properties analysisProperties = Config.getAnalysisProperties();
    private static Configuration configuration;

    static {
        stateDic = new HashMap<String, String>();
        stateDic.put("r", RUNNING);
        stateDic.put("t", TRANSFERRED);
        stateDic.put("qw", QUEUED);
        stateDic.put("Eqw", ERROR);
    }

    public SgeManager(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "Configuration object cannot be null");
    }

    public static void queueJob(String toolName, String wumJobName, int wumUserId, String outdir, String commandLine)
            throws Exception {
        queueJob(toolName, wumJobName, wumUserId, outdir, commandLine, getQueueName(toolName));
    }

    public static void queueJob(String toolName, String wumJobName, int wumUserId, String outdir, String commandLine, String queue)
            throws Exception {
        queueJob(toolName, wumJobName, wumUserId, outdir, commandLine, queue, "");
    }

    public static void queueJob(String toolName, String wumJobName, int wumUserId, URI outdir, String commandLine, String queue, String logFileId)
            throws Exception {
        if (outdir.getScheme() != null && !outdir.getScheme().equals("file")) {
            throw new IOException("Unsupported outdir for QueueJob");
        }
        queueJob(toolName, wumJobName, wumUserId, outdir.getPath(), commandLine, queue, logFileId);
    }
    @Deprecated
    public static void queueJob(String toolName, String wumJobName, int wumUserId, String outdir, String commandLine, String queue, String logFileId)
            throws Exception {
        logFileId = logFileId == null || logFileId.isEmpty() ? "" : "." + logFileId;
        queue = queue == null || queue.isEmpty() ? getQueueName(toolName) : queue;
        String outFile = Paths.get(outdir, "sge_out" + logFileId + ".log").toString();
        String errFile = Paths.get(outdir, "sge_err" + logFileId + ".log").toString();

        // init sge job
        String outScript = outdir + "/command_line.sh";
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
        Command sgeCommand = new Command(cmdArray, Collections.EMPTY_LIST);
        SingleProcess sp = new SingleProcess(sgeCommand);
        sp.getRunnableProcess().run();
        if (sgeCommand.getExitValue() != 0 || sgeCommand.getException() != null) {
            throw new Exception("Can't queue job " + getSgeJobName(toolName, wumJobName) + ". qsub returned " + sgeCommand.getExitValue() + " and message:" + sgeCommand.getException());
        }
    }


    private static String getSgeJobName(String toolName, String wumJobId) {
        return toolName.replace(" ", "_") + "_" + wumJobId;
    }

    private static String getQueueName(String toolName) throws Exception {
        String defaultQueue = getDefaultQueue();
        logger.debug("SgeManager: default queue: " + defaultQueue);

        // get all available queues
        List<String> queueList = getQueueList();
        logger.debug("SgeManager: available queues: " + queueList);

        // search corresponding queue
        String selectedQueue = defaultQueue;
        for (String queue : queueList) {
            if (!queue.equalsIgnoreCase(defaultQueue)) {
                if (configuration.getExecution().getToolsPerQueue().get(queue) != null) {
                    if (belongsTheToolToQueue(configuration.getExecution().getToolsPerQueue().get(queue), toolName)) {
                        selectedQueue = queue;
                    }
                }
            }
        }
        logger.info("SgeManager: selected queue for tool '" + toolName + "': " + selectedQueue);
        return selectedQueue;
    }

    private static String getDefaultQueue() throws Exception {
        if (StringUtils.isEmpty(configuration.getExecution().getDefaultQueue())) {
            throw new Exception("Execution default queue is not defined!");
        }
        return configuration.getExecution().getDefaultQueue();
    }

    private static List<String> getQueueList() {
        if (configuration.getExecution().getAvailableQueues() != null) {
            String[] queueArray = configuration.getExecution().getAvailableQueues().split(",");
            return Arrays.asList(queueArray);
        } else {
            return new ArrayList<>();
        }
    }

    private static boolean belongsTheToolToQueue(String tools, String toolName) {
        List<String> toolList = Splitter.on(",").splitToList(tools);
//        List<String> toolList = StringUtils.toList(tools, ",");
        // System.err.println("Tool list : " + toolList);
        return toolList.contains(toolName);
    }

    public static String status(String jobId) throws Exception {
        String status = UNKNOWN;

        String xml = null;
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

        if (!status.equals(UNKNOWN)) {
            status = stateDic.get(status);
        } else {
            String command = "qacct -j *" + jobId + "*";
//            logger.info(command);
            Process p = Runtime.getRuntime().exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            String exitStatus = null;
            String failed = null;
            while ((line = in.readLine()) != null) {
//                logger.info(line);
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
                if (!"0".equals(failed)) {
                    status = "queue error";
                }
                if ("0".equals(exitStatus)) {
                    status = FINISHED;
                } else {
                    status = EXECUTION_ERROR;
                }
            }
        }
        return status;
    }

}

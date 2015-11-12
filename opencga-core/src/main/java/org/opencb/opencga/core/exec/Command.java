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

package org.opencb.opencga.core.exec;

import org.apache.tools.ant.types.Commandline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Command extends RunnableProcess {

    // protected String executable;
    // protected String pathScript;
    // protected String outDir;
    // protected Arguments arguments;

    private String commandLine;
    private Map<String, String> environment;
    private Process proc;
    private boolean clearEnvironment = false;

    protected static Logger logger = LoggerFactory.getLogger(Command.class);
    private StringBuffer outputBuffer = new StringBuffer();
    private StringBuffer errorBuffer = new StringBuffer();
    private OutputStream outputOutputStream = null;
    private OutputStream errorOutputStream = null;

    private final String[] cmdArray;

    public Command(String commandLine) {
        this.commandLine = commandLine;
        cmdArray = Commandline.translateCommandline(getCommandLine());
    }

    public Command(String commandLine, List<String> environment) {
        this.commandLine = commandLine;
        this.environment = parseEnvironment(environment);
        cmdArray = Commandline.translateCommandline(getCommandLine());
    }

    public Command(String[] cmdArray, List<String> environment) {
        this.cmdArray = cmdArray;
        this.commandLine = Commandline.toString(cmdArray);
        this.environment = parseEnvironment(environment);
    }

    @Override
    public void run() {
        try {
            setStatus(Status.RUNNING);

            startTime();
            logger.debug(Commandline.describeCommand(cmdArray));
            if (environment != null && environment.size() > 0) {
                ProcessBuilder processBuilder = new ProcessBuilder(cmdArray);
                if (clearEnvironment) {
                    processBuilder.environment().clear();
                }
                processBuilder.environment().putAll(environment);
//                logger.debug("Environment variables:");
//                processBuilder.environment().forEach((k, v) -> logger.debug("\t" + k + "=" + v));
                proc = processBuilder.start();
            } else {
                proc = Runtime.getRuntime().exec(cmdArray);
            }

            InputStream is = proc.getInputStream();
            // Thread out = readOutputStream(is);
            Thread readOutputStreamThread = readOutputStream(is);
            InputStream es = proc.getErrorStream();
            // Thread err = readErrorStream(es);
            Thread readErrorStreamThread = readErrorStream(es);

            proc.waitFor();
            readOutputStreamThread.join();
            readErrorStreamThread.join();
            endTime();

            setExitValue(proc.exitValue());
            if (proc.exitValue() != 0) {
                status = Status.ERROR;
                // output = IOUtils.toString(proc.getInputStream());
                // error = IOUtils.toString(proc.getErrorStream());
                output = outputBuffer.toString();
                error = errorBuffer.toString();
            }
            if (status != Status.KILLED && status != Status.TIMEOUT && status != Status.ERROR) {
                status = Status.DONE;
                // output = IOUtils.toString(proc.getInputStream());
                // error = IOUtils.toString(proc.getErrorStream());
                output = outputBuffer.toString();
                error = errorBuffer.toString();
            }

        } catch (Exception e) {
            exception = e.toString();
            status = Status.ERROR;
            exitValue = -1;
            logger.error("Exception occurred while executing Command {}", exception);
        }
    }

    @Override
    public void destroy() {
        proc.destroy();
    }

    private Thread readOutputStream(InputStream ins) throws IOException {
        final InputStream in = ins;

        Thread T = new Thread("stdout_reader") {
            public void run() {
                try {
                    int bytesRead = 0;
                    int bufferLength = 2048;
                    byte[] buffer = new byte[bufferLength];

                    while (bytesRead != -1) {
                        // int x=in.available();
                        bufferLength = in.available();
                        bufferLength = Math.max(bufferLength, 1);
                        // if (x<=0)
                        // continue ;

                        buffer = new byte[bufferLength];
                        bytesRead = in.read(buffer, 0, bufferLength);
                        if (logger != null) {
                            System.err.print(new String(buffer));
                        }
                        outputBuffer.append(new String(buffer));
                        if (outputOutputStream != null) {
                            outputOutputStream.write(buffer);
                            outputOutputStream.flush();
                        }
                        Thread.sleep(500);
                        logger.trace("stdout - Sleep (last bytesRead = " + bytesRead + ")");
                    }
                    logger.debug("ReadOutputStream - Exit while");
                } catch (Exception ex) {
                    ex.printStackTrace();
                    exception = ex.toString();
                }
            }
        };
        T.start();
        return T;
    }

    private Thread readErrorStream(InputStream ins) throws IOException {
        final InputStream in = ins;

        Thread T = new Thread("stderr_reader") {
            public void run() {

                try {
                    int bytesRead = 0;
                    int bufferLength = 2048;
                    byte[] buffer = new byte[bufferLength];

                    while (bytesRead != -1) {
                        // int x=in.available();
                        // if (x<=0)
                        // continue ;

                        bufferLength = in.available();
                        bufferLength = Math.max(bufferLength, 1);

                        buffer = new byte[bufferLength];
                        bytesRead = in.read(buffer, 0, bufferLength);
                        if (logger != null) {
                            System.err.print(new String(buffer));
                        }
                        errorBuffer.append(new String(buffer));
                        if (errorOutputStream != null) {
                            errorOutputStream.write(buffer);
                            errorOutputStream.flush();
                        }
                        Thread.sleep(500);
                        logger.trace("stderr - Sleep  (last bytesRead = " + bytesRead + ")");
                    }
                    logger.debug("ReadErrorStream - Exit while");
                } catch (Exception ex) {
                    ex.printStackTrace();
                    exception = ex.toString();
                }
            }
        };
        T.start();
        return T;
    }

    /**
     * @param commandLine the commandLine to set
     */
    public void setCommandLine(String commandLine) {
        this.commandLine = commandLine;
    }

    /**
     * @return the commandLine
     */
    public String getCommandLine() {
        return commandLine;
    }

    /**
     * @param environment the environment to set
     */
    public void setEnvironment(List<String> environment) {
        this.environment = parseEnvironment(environment);
    }

    /**
     * @return the environment
     */
    public List<String> getEnvironment() {
        return Collections.unmodifiableList(
                environment.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.toList()));
    }

    /**
     * @param environment the environment to set
     */
    public void setEnvironmentMap(Map<String, String> environment) {
        this.environment = environment;
    }

    /**
     * @return the environment as map
     */
    public Map<String, String> getEnvironmentMap() {
        return environment;
    }

    public boolean isClearEnvironment() {
        return clearEnvironment;
    }

    public Command setClearEnvironment(boolean clearEnvironment) {
        this.clearEnvironment = clearEnvironment;
        return this;
    }

    private Map<String, String> parseEnvironment(List<String> environmentList) {
        Map<String, String> environment = new HashMap<>();
        for (String s : environmentList) {
            String[] split = s.split("=");
            environment.put(split[0], split[1]);
        }
        return environment;
    }

    public OutputStream getOutputOutputStream() {
        return outputOutputStream;
    }

    public Command setOutputOutputStream(OutputStream outputOutputStream) {
        this.outputOutputStream = outputOutputStream;
        return this;
    }

    public OutputStream getErrorOutputStream() {
        return errorOutputStream;
    }

    public Command setErrorOutputStream(OutputStream errorOutputStream) {
        this.errorOutputStream = errorOutputStream;
        return this;
    }
}

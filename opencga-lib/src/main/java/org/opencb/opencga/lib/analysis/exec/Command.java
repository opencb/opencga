package org.opencb.opencga.lib.analysis.exec;

import org.opencb.opencga.lib.common.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class Command extends RunnableProcess {

    // protected String executable;
    // protected String pathScript;
    // protected String outDir;
    // protected Arguments arguments;

    private String commandLine;
    private List<String> environment;
    private Process proc;

    protected static Logger logger = LoggerFactory.getLogger(Command.class);
    private StringBuffer outputBuffer = new StringBuffer();
    private StringBuffer errorBuffer = new StringBuffer();

    public Command() {

    }

    public Command(String commandLine) {
        this.commandLine = commandLine;
    }

    public Command(String commandLine, List<String> environment) {
        this.commandLine = commandLine;
        this.environment = environment;
    }

    @Override
    public void run() {
        try {
            setStatus(Status.RUNNING);

            startTime();
            if (environment != null && environment.size() > 0) {
                proc = Runtime.getRuntime().exec(getCommandLine(), ListUtils.toArray(environment));
            } else {
                proc = Runtime.getRuntime().exec(getCommandLine());
            }

            InputStream is = proc.getInputStream();
            // Thread out = readOutputStream(is);
            readOutputStream(is);
            InputStream es = proc.getErrorStream();
            // Thread err = readErrorStream(es);
            readErrorStream(es);

            proc.waitFor();
            endTime();

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

        } catch (IOException ioe) {
            exception = ioe.toString();
            status = Status.ERROR;
        } catch (InterruptedException e) {
            exception = e.toString();
            status = Status.ERROR;
        } catch (Exception e) {
            exception = e.toString();
            status = Status.ERROR;
        }

    }

    @Override
    public void destroy() {
        proc.destroy();
    }

    private void readOutputStream(InputStream ins) throws IOException {
        final InputStream in = ins;

        Thread T = new Thread("output_reader") {
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
                            logger.info(new String(buffer));
                        }
                        outputBuffer.append(new String(buffer));
                        Thread.sleep(500);
                        System.err.println("Output- Sleep (last bytesRead = " + bytesRead + ")");
                    }
                    System.err.println("Output - Fuera while");
                } catch (Exception ex) {
                    ex.printStackTrace();
                    exception = ex.toString();
                }
            }
        };
        T.start();
        // return T;
    }

    private void readErrorStream(InputStream ins) throws IOException {
        final InputStream in = ins;

        Thread T = new Thread("errror_reader") {
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
                            logger.info(new String(buffer));
                        }
                        errorBuffer.append(new String(buffer));
                        Thread.sleep(500);
                        System.err.println("Error- Sleep  (last bytesRead = " + bytesRead + ")");
                    }
                    System.err.println("Error - Fuera while");
                } catch (Exception ex) {
                    ex.printStackTrace();
                    exception = ex.toString();
                }
            }
        };
        T.start();
        // return T;
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
        this.environment = environment;
    }

    /**
     * @return the environment
     */
    public List<String> getEnvironment() {
        return environment;
    }

    /**
     * @param logger the logger to set
     */
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    /**
     * @return the logger
     */
    public Logger getLogger() {
        return logger;
    }

}

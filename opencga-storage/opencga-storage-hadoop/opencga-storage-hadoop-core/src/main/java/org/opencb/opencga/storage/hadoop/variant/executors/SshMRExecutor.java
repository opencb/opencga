package org.opencb.opencga.storage.hadoop.variant.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.RunJar;
import org.apache.tools.ant.types.Commandline;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.exec.Command;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDriver;
import org.opencb.opencga.storage.hadoop.utils.MapReduceOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SshMRExecutor extends MRExecutor {

    private static final String HADOOP_SSH_USER_ENV = "HADOOP_SSH_USER";
    private static final String HADOOP_SSH_HOST_ENV = "HADOOP_SSH_HOST";
    private static final String HADOOP_SSH_KEY_ENV  = "HADOOP_SSH_KEY";
    private static final String HADOOP_SSH_REMOTE_TMP  = "HADOOP_SSH_REMOTE_TMP";
    private static final String HADOOP_SSH_STDIN_ENABLED  = "HADOOP_SSH_STDIN_ENABLED";
    // env-var expected by "sshpass -e"
    private static final String SSHPASS_ENV = "SSHPASS";
    public static final String PID = "PID";
    private static final Logger LOGGER = LoggerFactory.getLogger(SshMRExecutor.class);

    @Override
    public SshMRExecutor init(String dbName, Configuration conf, ObjectMap options) {
        super.init(dbName, conf, options);
        return this;
    }

    @Override
    public Result run(String executable, String[] args) throws StorageEngineException {
        MapReduceOutputFile mrOutput = initMrOutput(executable, args);
        List<String> env = buildEnv();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Thread hook = new Thread(() -> {
            LOGGER.info("Shutdown hook. Killing MR jobs");
            LOGGER.info("Read output key-value:");
            ObjectMap result = readResult(new String(outputStream.toByteArray(), Charset.defaultCharset()));
            for (Map.Entry<String, Object> entry : result.entrySet()) {
                LOGGER.info(" - " + entry.getKey() + ": " + entry.getValue());
            }
            String remotePid = result.getString(PID);
            LOGGER.info("Remote PID: " + remotePid);
            List<String> mrJobs = result.getAsStringList(AbstractHBaseDriver.MR_APPLICATION_ID);
            LOGGER.info("MR jobs to kill: " + mrJobs);
            for (String mrJob : mrJobs) {
                LOGGER.info("Killing MR job " + mrJob);
                String commandLineKill = buildCommand("yarn", "application", "-kill", mrJob);
                Command command = new Command(commandLineKill, env);
                command.run();
                int exitValue = command.getExitValue();
                if (exitValue != 0) {
                    LOGGER.error("Error killing MR job " + mrJob);
                } else {
                    LOGGER.info("MR job " + mrJob + " killed!");
                }
            }
            if (remotePid != null) {
                int remoteProcessGraceKillPeriod = getOptions()
                        .getInt(MR_EXECUTOR_SSH_HADOOP_TERMINATION_GRACE_PERIOD_SECONDS.key(),
                                MR_EXECUTOR_SSH_HADOOP_TERMINATION_GRACE_PERIOD_SECONDS.defaultValue());
                LOGGER.info("Wait up to " + remoteProcessGraceKillPeriod + "s for the remote process to finish");
                String commandLineWaitPid = buildCommand("bash", "-c", ""
                        + "pid=" + remotePid + "; "
                        + "i=0; "
                        + "while [ $(( i++ )) -lt " + remoteProcessGraceKillPeriod + " ] && ps -p $pid > /dev/null; do sleep 1; done; "
                        + "if ps -p $pid > /dev/null; then "
                        + "   echo \"Kill remote ssh process $pid\"; "
                        + "   ps -p $pid; "
                        + "   kill -15 $pid; "
                        + "else "
                        + "   echo \"Process $pid finished\"; "
                        + " fi");
                Command command = new Command(commandLineWaitPid, env);
                command.run();
                if (command.getExitValue() != 0) {
                    LOGGER.error("Error waiting for remote process to finish");
                } else {
                    LOGGER.info("Remote process finished!");
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        int exitValue;
        try {
            exitValue = runRemote(executable, args, env, outputStream);
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (Exception e) {
                logger.error("Error removing shutdown hook", e);
            }
        }
        boolean succeed = exitValue == 0;
        ObjectMap result = readResult(new String(outputStream.toByteArray(), Charset.defaultCharset()));
        try {
            if (succeed) {
                if (mrOutput != null) {
                    mrOutput.postExecute(result, succeed);
                } else {
                    copyOutputFiles(args, env);
                }
                // Copy extra output files
                for (String key : result.keySet()) {
                    if (key.startsWith(MapReduceOutputFile.EXTRA_OUTPUT_PREFIX)) {
                        copyOutputFiles(result.getString(key), env);
                    }
                }
            } else {
                if (mrOutput != null) {
                    mrOutput.postExecute(result, succeed);
                } // else // should delete remote output files?
            }
        } catch (IOException e) {
            throw new StorageEngineException(e.getMessage(), e);
        }
        return new Result(exitValue, result);
    }

    protected int runRemote(String executable, String[] args, List<String> env, ByteArrayOutputStream outputStream) {
        String commandLine = buildCommand(executable, args);
        Command command;
        if (commandLine.length() > MAX_COMMAND_LINE_ARGS_LENGTH) {
            LOGGER.info("Command line is too long. Passing args through stdin");
            commandLine = buildCommand(executable, AbstractHBaseDriver.ARGS_FROM_STDIN);
            redactSecureString(args, MR_EXECUTOR_SSH_PASSWORD.key());
            redactSecureString(args, "token");
            env.add(HADOOP_SSH_STDIN_ENABLED + "=true");

            command = new Command(commandLine, env);
            command.setErrorOutputStream(outputStream);
            runWithArgsToStdin(command, args);
        } else {
            command = new Command(commandLine, env);
            command.setErrorOutputStream(outputStream);
            command.run();
        }
        return command.getExitValue();
    }

    /**
     * If the MapReduce to be executed is writing to a local filesystem, change the output to a temporary HDFS path.
     * The output will be copied to the local filesystem after the execution.
     * <p>
     *     This method will look for the ${@link AbstractHBaseDriver#OUTPUT_PARAM} argument in the args array.
     *
     * @param executable    Executable
     * @param args          Arguments passed to the executable. Might be modified
     * @return              MapReduceOutputFile if any
     * @throws StorageEngineException if there is an issue creating the temporary output path
     */
    private MapReduceOutputFile initMrOutput(String executable, String[] args) throws StorageEngineException {
        MapReduceOutputFile mrOutput = null;
        List<String> argsList = Arrays.asList(args);
        int outputIdx = argsList.indexOf(AbstractHBaseDriver.OUTPUT_PARAM);
        if (outputIdx > 0 && argsList.size() > outputIdx + 1) {
            String output = argsList.get(outputIdx + 1);
            URI outputUri = UriUtils.createUriSafe(output);
            if (MapReduceOutputFile.isLocal(outputUri)) {
                LOGGER.info("This MapReduce will produce some output. Change output location from file:// to a temporary hdfs:// file"
                        + " so it can be copied to the local filesystem after the execution");
                try {
                    int i = executable.lastIndexOf('.');
                    String tempFilePrefix;
                    if (i > 0) {
                        String className = executable.substring(i);
                        tempFilePrefix = dbName + className;
                    } else {
                        tempFilePrefix = dbName;
                    }
                    mrOutput = new MapReduceOutputFile(outputUri.toString(), null,
                            tempFilePrefix, true, conf);
                } catch (IOException e) {
                    throw new StorageEngineException(e.getMessage(), e);
                }
                // Replace output path with the temporary path
                argsList.set(outputIdx + 1, mrOutput.getOutdir().toString());
            }
        }
        return mrOutput;
    }

    /**
     * Copy output files from remote server to local filesystem.
     * <p>
     * This method will look for the "output" argument in the args array.
     * The value of the argument is expected to be a path.
     *
     * @param args Arguments passed to the executable
     * @param env  Environment variables
     * @return Path to the output file
     * @throws StorageEngineException if there is an issue copying the files
     */
    private Path copyOutputFiles(String[] args, List<String> env) throws StorageEngineException {
        List<String> argsList = Arrays.asList(args);
        int outputIdx = argsList.indexOf(AbstractHBaseDriver.OUTPUT_PARAM);
        if (outputIdx > 0 && argsList.size() > outputIdx + 1) {
            return copyOutputFiles(argsList.get(outputIdx + 1), env);
        }
        // Nothing to copy
        return null;
    }

    private Path copyOutputFiles(String output, List<String> env) throws StorageEngineException {
        URI targetOutputUri = UriUtils.createUriSafe(output);
        if (!MapReduceOutputFile.isLocal(targetOutputUri)) {
            LOGGER.info("Output is not a file:// URI. Skipping copy file {}", targetOutputUri);
            return null;
        }
        String targetOutput = targetOutputUri.getPath();
        if (StringUtils.isNotEmpty(targetOutput)) {
            String remoteOpencgaHome = getOptions().getString(MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME.key());
            String srcOutput;
            if (StringUtils.isNoneEmpty(remoteOpencgaHome, getOpencgaHome())) {
                srcOutput = targetOutput.replaceAll(getOpencgaHome(), remoteOpencgaHome);
            } else {
                srcOutput = targetOutput;
            }

            String hadoopScpBin = getOptions()
                    .getString(MR_EXECUTOR_SSH_HADOOP_SCP_BIN.key(), MR_EXECUTOR_SSH_HADOOP_SCP_BIN.defaultValue());
            String commandLine = getBinPath(hadoopScpBin) + " " + srcOutput + " " + targetOutput;

            Command command = new Command(commandLine, env);
            command.run();
            int exitValue = command.getExitValue();
            if (exitValue != 0) {
                String sshHost = getOptions().getString(MR_EXECUTOR_SSH_HOST.key());
                String sshUser = getOptions().getString(MR_EXECUTOR_SSH_USER.key());
                throw new StorageEngineException("There was an issue copying files from "
                        + sshUser + "@" + sshHost + ":" + srcOutput + " to " + targetOutput);
            }
            return Paths.get(targetOutput);
        }
        return null;
    }

    protected String buildCommand(String executable, String... args) {
        redactSecureString(args, MR_EXECUTOR_SSH_PASSWORD.key());
        redactSecureString(args, "token");
        String argsString = Commandline.toString(args);
        String remoteOpencgaHome = getOptions().getString(MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME.key());
        String hadoopSshBin = getOptions()
                .getString(MR_EXECUTOR_SSH_HADOOP_SSH_BIN.key(), MR_EXECUTOR_SSH_HADOOP_SSH_BIN.defaultValue());
        String commandLine = getBinPath(hadoopSshBin);

        if (StringUtils.isNotEmpty(remoteOpencgaHome)) {
            argsString = argsString.replaceAll(getOpencgaHome(), remoteOpencgaHome);
            executable = executable.replaceAll(getOpencgaHome(), remoteOpencgaHome);
        }

        commandLine += ' ' + executable + ' ' + argsString;
        return commandLine;
    }

    private String getBinPath(String bin) {
        String commandLine;
        String opencgaHome = getOpencgaHome();
        if (opencgaHome.isEmpty()) {
            commandLine = bin;
        } else if (Paths.get(bin).isAbsolute()) {
            commandLine = bin;
        } else {
            commandLine = Paths.get(opencgaHome, bin).toString();
        }
        return commandLine;
    }

    protected List<String> buildEnv() {
        String sshHost = getOptions().getString(MR_EXECUTOR_SSH_HOST.key());
        String sshUser = getOptions().getString(MR_EXECUTOR_SSH_USER.key());
        String sshPassword = getOptions().getString(MR_EXECUTOR_SSH_PASSWORD.key());
        String sshKey = getOptions().getString(MR_EXECUTOR_SSH_KEY.key());
        String remoteOpencgaHome = getOptions().getString(MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME.key());
        String remoteTmp = getOptions().getString(MR_EXECUTOR_SSH_REMOTE_TMP.key(), MR_EXECUTOR_SSH_REMOTE_TMP.defaultValue());

        if (StringUtils.isEmpty(sshHost)) {
            throw new IllegalArgumentException("Missing ssh credentials to run MapReduce job. Missing " + MR_EXECUTOR_SSH_HOST.key());
        }
        if (StringUtils.isEmpty(sshUser)) {
            throw new IllegalArgumentException("Missing ssh credentials to run MapReduce job. Missing " + MR_EXECUTOR_SSH_USER.key());
        }

        List<String> env = new ArrayList<>(getEnv());
        env.add(HADOOP_SSH_USER_ENV + '=' + sshUser);
        env.add(HADOOP_SSH_HOST_ENV + '=' + sshHost);
        env.add(HADOOP_SSH_REMOTE_TMP + '=' + remoteTmp);

        // Use sshpass to connect to the edge node. Otherwise, assume that there is a ssh-key in the system
        if (StringUtils.isNotEmpty(sshPassword)) {
            env.add(SSHPASS_ENV + '=' + sshPassword);
        }
        if (StringUtils.isNotEmpty(remoteOpencgaHome)) {
            String hadoopClasspath = System.getenv(RunJar.HADOOP_CLASSPATH);
            String remoteHadoopClasspath = replaceOpencgaHome(getOpencgaHome(), remoteOpencgaHome, hadoopClasspath);
            env.add(RunJar.HADOOP_CLASSPATH + '=' + remoteHadoopClasspath);
        }

        if (StringUtils.isNotEmpty(sshKey)) {
            Path sshKeyPath = Paths.get(sshKey);
            if (sshKeyPath.toFile().exists()) {
                env.add(HADOOP_SSH_KEY_ENV + '=' + sshKey);
            } else {
                throw new IllegalArgumentException("ssh key file '" + sshKey + "' does not exist!");
            }
        }
        return env;
    }

    protected String replaceOpencgaHome(String opencgaHome, String remoteOpencgaHome, String hadoopClasspath) {
        StringBuilder remoteHadoopClasspath = new StringBuilder();
        if (!opencgaHome.endsWith("/")) {
            opencgaHome += "/";
        }
        if (!remoteOpencgaHome.endsWith("/")) {
            remoteOpencgaHome += "/";
        }
        for (String classPath : hadoopClasspath.split(":")) {
            if (!classPath.isEmpty()) {
                if (remoteHadoopClasspath.length() > 0) {
                    remoteHadoopClasspath.append(':');
                }
                if (classPath.startsWith(opencgaHome)) {
                    remoteHadoopClasspath.append(classPath.replaceFirst(opencgaHome, remoteOpencgaHome));
                } else {
                    remoteHadoopClasspath.append(classPath);
                }
            }
        }
        return remoteHadoopClasspath.toString();
    }
}

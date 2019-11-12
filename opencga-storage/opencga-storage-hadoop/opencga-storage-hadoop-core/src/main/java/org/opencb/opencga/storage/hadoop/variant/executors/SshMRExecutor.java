package org.opencb.opencga.storage.hadoop.variant.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.RunJar;
import org.opencb.commons.exec.Command;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageOptions.*;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SshMRExecutor extends MRExecutor {

    public static final String HADOOP_SSH_BIN =  "hadoop-ssh.sh";

    private static final String HADOOP_SSH_USER_ENV = "HADOOP_SSH_USER";
    private static final String HADOOP_SSH_HOST_ENV = "HADOOP_SSH_HOST";
    private static final String HADOOP_SSH_KEY_ENV  = "HADOOP_SSH_KEY";
    // env-var expected by "sshpass -e"
    private static final String SSHPASS_ENV = "SSHPASS";

    @Override
    public int run(String executable, String args) {
        String commandLine = buildCommand(executable, args);
        List<String> env = buildEnv();

        Command command = new Command(commandLine, env);
        command.run();
        return command.getExitValue();
    }

    protected String buildCommand(String executable, String args) {
        String remoteOpencgaHome = getOptions().getString(MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME.key());
        String commandLine;
        String opencgaHome = getOpencgaHome();
        if (opencgaHome.isEmpty()) {
            commandLine = HADOOP_SSH_BIN;
        } else {
            commandLine = opencgaHome + "/conf/hadoop/" + HADOOP_SSH_BIN;
        }

        if (StringUtils.isNotEmpty(remoteOpencgaHome)) {
            args = args.replaceAll(getOpencgaHome(), remoteOpencgaHome);
            executable = executable.replaceAll(getOpencgaHome(), remoteOpencgaHome);
        }

        commandLine += ' ' + executable + ' ' + args;
        return commandLine;
    }

    protected List<String> buildEnv() {
        String sshHost = getOptions().getString(MR_EXECUTOR_SSH_HOST.key());
        String sshUser = getOptions().getString(MR_EXECUTOR_SSH_USER.key());
        String sshPassword = getOptions().getString(MR_EXECUTOR_SSH_PASSWORD.key());
        String sshKey = getOptions().getString(MR_EXECUTOR_SSH_KEY.key());
        String remoteOpencgaHome = getOptions().getString(MR_EXECUTOR_SSH_REMOTE_OPENCGA_HOME.key());

        if (StringUtils.isEmpty(sshHost)) {
            throw new IllegalArgumentException("Missing ssh credentials to run MapReduce job. Missing " + MR_EXECUTOR_SSH_HOST.key());
        }
        if (StringUtils.isEmpty(sshUser)) {
            throw new IllegalArgumentException("Missing ssh credentials to run MapReduce job. Missing " + MR_EXECUTOR_SSH_USER.key());
        }

        List<String> env = new ArrayList<>(getEnv());
        env.add(HADOOP_SSH_USER_ENV + '=' + sshUser);
        env.add(HADOOP_SSH_HOST_ENV + '=' + sshHost);

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

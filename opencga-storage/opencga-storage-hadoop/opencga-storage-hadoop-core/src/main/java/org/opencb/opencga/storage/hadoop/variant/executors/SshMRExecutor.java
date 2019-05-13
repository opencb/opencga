package org.opencb.opencga.storage.hadoop.variant.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.RunJar;
import org.opencb.commons.exec.Command;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SshMRExecutor extends MRExecutor {

    public static final String SSH_HOST =       "opencga.mr.executor.ssh.host";
    public static final String SSH_USER =       "opencga.mr.executor.ssh.user";
    public static final String SSH_KEY =        "opencga.mr.executor.ssh.key";
    public static final String SSH_PASSWORD =   "opencga.mr.executor.ssh.password";
    public static final String REMOTE_OPENCGA_HOME = "opencga.mr.executor.ssh.remote_opencga_home";
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
        String remoteOpencgaHome = getOptions().getString(REMOTE_OPENCGA_HOME);
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
        String sshHost = getOptions().getString(SSH_HOST);
        String sshUser = getOptions().getString(SSH_USER);
        String sshPassword = getOptions().getString(SSH_PASSWORD);
        String sshKey = getOptions().getString(SSH_KEY);
        String remoteOpencgaHome = getOptions().getString(REMOTE_OPENCGA_HOME);

        if (StringUtils.isEmpty(sshHost)) {
            throw new IllegalArgumentException("Missing ssh credentials to run MapReduce job. Missing " + SSH_HOST);
        }
        if (StringUtils.isEmpty(sshUser)) {
            throw new IllegalArgumentException("Missing ssh credentials to run MapReduce job. Missing " + SSH_USER);
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

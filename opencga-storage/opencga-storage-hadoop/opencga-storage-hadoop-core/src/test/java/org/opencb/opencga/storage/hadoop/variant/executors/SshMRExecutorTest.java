package org.opencb.opencga.storage.hadoop.variant.executors;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.util.List;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Created on 14/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SshMRExecutorTest {

    @Test
    public void testRun() throws StorageEngineException {
        System.setProperty("app.home", "/opt/opencga");
        ObjectMap options = new ObjectMap(MRExecutorFactory.MR_EXECUTOR, "ssh");
        options.put(SshMRExecutor.SSH_USER, "test_user");
        options.put(SshMRExecutor.SSH_HOST, "test_host");

        MRExecutor mrExecutor = MRExecutorFactory.getMRExecutor(options);

        assertThat(mrExecutor, instanceOf(SshMRExecutor.class));

        SshMRExecutor sshMRExecutor = (SshMRExecutor) mrExecutor;
        String cmd = sshMRExecutor.buildCommand("echo", "hello world");
        assertEquals("/opt/opencga/conf/hadoop/hadoop-ssh.sh echo hello world", cmd);

        List<String> env = sshMRExecutor.buildEnv();

        assertThat(env, hasItem("HADOOP_SSH_USER=test_user"));
        assertThat(env, hasItem("HADOOP_SSH_HOST=test_host"));
    }
}
package org.opencb.opencga.storage.hadoop.variant;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.core.exec.Command;

import java.util.List;

/**
 * Created on 18/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ExternalMRExecutor implements MRExecutor {
    private final List<String> env;

    public ExternalMRExecutor(ObjectMap options) {
        env = options.getAsStringList(HadoopVariantStorageManager.HADOOP_ENV);
    }

    @Override
    public int run(String executable, String args) {
        return run(executable + " " + args);
    }

    public int run(String commandLine) {
        Command command = new Command(commandLine, env);
        command.run();
        return command.getExitValue();
    }
}

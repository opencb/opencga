package org.opencb.opencga.storage.hadoop.variant;

/**
 * Created on 18/01/16 .
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface MRExecutor {

    int run(String executable, String args);

}

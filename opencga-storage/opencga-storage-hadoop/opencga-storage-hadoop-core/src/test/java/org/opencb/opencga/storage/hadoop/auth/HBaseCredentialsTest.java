package org.opencb.opencga.storage.hadoop.auth;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created on 05/10/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseCredentialsTest {


    @Test
    public void testToString() {

        HBaseCredentials credentials = new HBaseCredentials("node1.hdp,node2.hdp:2181", null, null, null, null, "/hbase-unsecure");

        String s = credentials.toString();

        HBaseCredentials credentials2 = new HBaseCredentials(s);

//        System.out.println("credentials = " + credentials);
//        System.out.println("credentials2 = " + credentials2);
        Assert.assertEquals(credentials, credentials2);
        Assert.assertEquals(credentials.toString(), credentials2.toString());

    }

}

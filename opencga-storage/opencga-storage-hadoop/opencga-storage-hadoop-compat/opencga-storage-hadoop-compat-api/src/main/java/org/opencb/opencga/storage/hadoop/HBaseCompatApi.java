package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompatApi;

import java.io.IOException;
import java.util.List;

public abstract class HBaseCompatApi {

    // singleton
    private static HBaseCompatApi instance;
    public static HBaseCompatApi getInstance() {
        if (instance == null) {
            try {
                instance = Class.forName("org.opencb.opencga.storage.hadoop.HBaseCompat")
                        .asSubclass(HBaseCompatApi.class)
                        .getDeclaredConstructor()
                        .newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
        return instance;
    }

    public abstract PhoenixCompatApi getPhoenixCompat();

    public abstract void available(Configuration configuration) throws IOException;

    public abstract boolean isSolrTestingAvailable();

    public abstract List<ServerName> getServerList(Admin admin) throws IOException;

    public abstract byte[][] getTableStartKeys(Admin admin, Table table) throws IOException;

    public abstract boolean isSnappyAvailable();

    public abstract Class<?>[] getClassesForDependencyJars();
}

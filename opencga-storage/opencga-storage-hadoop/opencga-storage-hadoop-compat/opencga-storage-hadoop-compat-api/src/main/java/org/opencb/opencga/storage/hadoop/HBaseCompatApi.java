package org.opencb.opencga.storage.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.opencb.opencga.storage.hadoop.variant.annotation.phoenix.PhoenixCompatApi;

import java.io.IOException;

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

    public abstract void available(Configuration configuration) throws IOException;

    public abstract PhoenixCompatApi getPhoenixCompat();

}

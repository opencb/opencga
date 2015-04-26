package org.opencb.opencga.catalog.beans;

import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by imedina on 13/09/14.
 */
public class Metadata {

    private String version;
    private String date;
    private String open;

    private int idCounter;


    public Metadata() {
        this("v2", TimeUtils.getTime(), "public");
    }

    public Metadata(String version, String date, String open) {
        this(version, date, open, 0);
    }

    public Metadata(String version, String date, String open, int idCounter) {
        this.version = version;
        this.date = date;
        this.open = open;
        this.idCounter = idCounter;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "version='" + version + '\'' +
                ", date='" + date + '\'' +
                ", open='" + open + '\'' +
                ", idCounter=" + idCounter +
                '}';
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }

    public int getIdCounter() {
        return idCounter;
    }

    public void setIdCounter(int idCounter) {
        this.idCounter = idCounter;
    }
}

package org.opencb.opencga.catalog.models;

import org.opencb.opencga.core.common.TimeUtils;

/**
 * Created by pfurio on 11/03/16.
 */
public class Status {

    private String status;
    private String date;
    private String msg;

    public Status() {
        this.status = "active";
        this.date = TimeUtils.getTimeMillis();
    }

    public Status(String status, String msg) {
        this.status = status;
        this.date = TimeUtils.getTimeMillis();
        this.msg = msg;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDate() {
        return date;
    }

    public void setDate() {
        this.date = TimeUtils.getTimeMillis();
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return status;
    }
}

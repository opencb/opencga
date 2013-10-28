package org.opencb.opencga.account.beans;

import org.opencb.opencga.lib.common.TimeUtils;

import java.util.ArrayList;
import java.util.List;


public class ObjectItem {

    private String id;
    private String fileName;
    private String fileType;
    private String fileFormat;
    private String fileBioType;
    private long diskUsage;
    private String status;
    private String date;
    private String creationTime;
    private String responsible;
    private String organization;
    private String description;
    private List<Acl> acl;

    public static String UPLOADING = "uploading";
    public static String UPLOADED = "uploaded";
    public static String READY = "ready";


    public ObjectItem(String id, String fileName, String fileType) {
        this(id, fileName, fileType, "", "", 0, ObjectItem.READY, "", TimeUtils.getTime(), "", "", "", new ArrayList<Acl>());
    }

    public ObjectItem(String id, String fileName, String fileType, String fileFormat, String fileBioType,
                      long diskUsage, String date, String responsible, String organization, String description, List<Acl> acl) {
        this(id, fileName, fileType, fileFormat, fileBioType, diskUsage, ObjectItem.READY, date, TimeUtils.getTime(), responsible, organization, description, acl);
    }


    public ObjectItem(String id, String fileName, String fileType, String fileFormat, String fileBioType, long diskUsage,
                      String status, String date, String creationTime, String responsible, String organization, String description, List<Acl> acl) {
        this.id = id;
        this.fileName = fileName;
        this.fileType = fileType;
        this.fileFormat = fileFormat;
        this.fileBioType = fileBioType;
        this.diskUsage = diskUsage;
        this.status = status;
        this.date = date;
        this.creationTime = creationTime;
        this.responsible = responsible;
        this.organization = organization;
        this.description = description;
        this.acl = acl;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }


    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }


    public String getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
    }


    public String getFileBioType() {
        return fileBioType;
    }

    public void setFileBioType(String fileBioType) {
        this.fileBioType = fileBioType;
    }


    public long getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
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

    public void setDate(String date) {
        this.date = date;
    }


    public String getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(String creationTime) {
        this.creationTime = creationTime;
    }


    public String getResponsible() {
        return responsible;
    }

    public void setResponsible(String responsible) {
        this.responsible = responsible;
    }


    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }


    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }


    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }


}

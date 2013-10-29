package org.opencb.opencga.account.beans;

import org.opencb.opencga.lib.common.TimeUtils;

import java.util.ArrayList;
import java.util.List;


public class Bucket {

    private String id;
    private String name;
    private String status;
    private String diskUsage;
    private String creationDate;
    private String ownerId;
    private String type;
    private String descripcion;
    private List<Acl> acl;
    private List<ObjectItem> objects;


    public Bucket() {

    }

    public Bucket(String name) {
        this(name.toLowerCase(), name, "1", "", TimeUtils.getTime(), "", "", "", new ArrayList<Acl>(), new ArrayList<ObjectItem>());
    }

    public Bucket(String id, String name, String status, String diskUsage, String ownerId, String type, String descripcion, List<Acl> acl) {
        this(id, name, status, diskUsage, TimeUtils.getTime(), ownerId, type, descripcion, acl, new ArrayList<ObjectItem>());
    }

    public Bucket(String id, String name, String status, String diskUsage, String creationDate, String ownerId, String type, String descripcion, List<Acl> acl, List<ObjectItem> objects) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.diskUsage = diskUsage;
        this.creationDate = (creationDate != null && !creationDate.equals("")) ? creationDate : TimeUtils.getTime();
        this.ownerId = ownerId;
        this.type = type;
        this.descripcion = descripcion;
        this.acl = acl;
        this.objects = objects;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDiskUsage() {
        return diskUsage;
    }

    public void setDiskUsage(String diskUsage) {
        this.diskUsage = diskUsage;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(String creationDate) {
        this.creationDate = creationDate;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }

    //	public List<ObjectItem> getData() {
//		return objects;
//	}
//
//	public void setData(List<ObjectItem> objectItemList) {
//		this.objects = objectItemList;
//	}
    public List<ObjectItem> getObjects() {
        return objects;
    }

    public void setObjects(List<ObjectItem> objects) {
        this.objects = objects;
    }


}

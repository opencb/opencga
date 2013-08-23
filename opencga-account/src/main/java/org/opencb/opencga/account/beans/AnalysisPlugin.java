package org.opencb.opencga.account.beans;

import java.util.List;

public class AnalysisPlugin {

    private String id;
    private String name;
    private String ownerId;
    private List<Acl> acl;

    public AnalysisPlugin(String id, String name, String ownerId, List<Acl> acl) {
        super();
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
        this.acl = acl;
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

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public List<Acl> getAcl() {
        return acl;
    }

    public void setAcl(List<Acl> acl) {
        this.acl = acl;
    }

}

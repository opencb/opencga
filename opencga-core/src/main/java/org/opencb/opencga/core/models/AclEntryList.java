package org.opencb.opencga.core.models;


import java.util.List;

public class AclEntryList<E extends Enum<E>> {

    protected String id;
    private List<AclEntry<E>> acl;

    public String getId() {
        return id;
    }

    public AclEntryList setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleAclEntryList{");
        sb.append("id='").append(id).append('\'');
        sb.append(", acl=").append(acl);
        sb.append('}');
        return sb.toString();
    }

    public List<AclEntry<E>> getAcl() {
        return acl;
    }

    public AclEntryList setAcl(List<AclEntry<E>> acl) {
        this.acl = acl;
        return this;
    }
}

package org.opencb.opencga.catalog.models.acls;

import java.util.EnumSet;

/**
 * Created by pfurio on 04/07/16.
 */
public abstract class AbstractAcl<E extends Enum<E>> {

    protected String member;
    protected EnumSet<E> permissions;

    public AbstractAcl() {
        this("", null);
    }

    public AbstractAcl(String member, EnumSet<E> permissions) {
        this.member = member;
        this.permissions = permissions;
    }

    public String getMember() {
        return member;
    }

    public AbstractAcl setMember(String member) {
        this.member = member;
        return this;
    }

    public EnumSet<E> getPermissions() {
        return permissions;
    }

    public AbstractAcl setPermissions(EnumSet<E> permissions) {
        this.permissions = permissions;
        return this;
    }
}

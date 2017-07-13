package org.opencb.opencga.catalog.models;

/**
 * Created by pfurio on 13/07/17.
 */
public class MemberParams {

    private String users;
    private Action action;

    public enum Action {
        ADD,
        REMOVE
    }

    public MemberParams() {
    }

    public MemberParams(String users, Action action) {
        this.users = users;
        this.action = action;
    }

    public String getUsers() {
        return users;
    }

    public MemberParams setUsers(String users) {
        this.users = users;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public MemberParams setAction(Action action) {
        this.action = action;
        return this;
    }

    public GroupParams toGroupParams() {
        return new GroupParams(this.users, GroupParams.Action.valueOf(this.action.name()));
    }

}

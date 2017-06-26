package org.opencb.opencga.catalog.models;

/**
 * Created by pfurio on 23/06/17.
 */
public class GroupParams {

    private String users;
    private Action action;

    public enum Action {
        SET,
        ADD,
        REMOVE
    }

    public GroupParams() {
    }

    public GroupParams(String users, Action action) {
        this.users = users;
        this.action = action;
    }

    public String getUsers() {
        return users;
    }

    public GroupParams setUsers(String users) {
        this.users = users;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public GroupParams setAction(Action action) {
        this.action = action;
        return this;
    }
}

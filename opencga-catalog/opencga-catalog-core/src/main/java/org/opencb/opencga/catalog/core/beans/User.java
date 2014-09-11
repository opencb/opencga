package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by imedina on 11/09/14.
 */
public class User {
    private String userId;
    private String userName;
    private String email;
    private String password;
    private String mailingList;
    private String organization;


    private List<Session> sessions;
    private List<Session> oldSessions;

    public User() {
    }

    public User(String userId, String userName, String email, String password, String mailingList, String organization, List<Session> sessions, List<Session> oldSessions) {
        this.userId = userId;
        this.userName = userName;
        this.email = email;
        this.password = password;
        this.mailingList = mailingList;
        this.organization = organization;
        this.sessions = sessions;
        this.oldSessions = oldSessions;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getMailingList() {
        return mailingList;
    }

    public void setMailingList(String mailingList) {
        this.mailingList = mailingList;
    }

    public String getOrganization() {
        return organization;
    }

    public void setOrganization(String organization) {
        this.organization = organization;
    }

    public List<Session> getSessions() {
        return sessions;
    }

    public void setSessions(List<Session> sessions) {
        this.sessions = sessions;
    }

    public List<Session> getOldSessions() {
        return oldSessions;
    }

    public void setOldSessions(List<Session> oldSessions) {
        this.oldSessions = oldSessions;
    }
}

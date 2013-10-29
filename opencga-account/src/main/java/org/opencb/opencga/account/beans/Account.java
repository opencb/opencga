package org.opencb.opencga.account.beans;

import java.util.ArrayList;
import java.util.List;

public class Account {

    private String accountId;
    private String accountName;
    private String email;
    private String password;
    private String role;
    private String status;
    private String mailingList;
    private long diskQuota;
    private long diskUsage;
    private String lastActivity;
    private List<Session> sessions = new ArrayList<Session>();
    private List<Session> oldSessions = new ArrayList<Session>();
    private List<Bucket> buckets = new ArrayList<Bucket>();
    private List<Credential> credentials = new ArrayList<Credential>();
    private List<AnalysisPlugin> plugins = new ArrayList<AnalysisPlugin>();
    private List<Config> configs = new ArrayList<Config>();
    private List<Project> projects = new ArrayList<>();

    public Account() {

    }

    public Account(String accountId, String accountName, String password, String role, String email) {
        this.accountId = accountId;
        this.accountName = accountName;
        this.email = email;
        this.password = password;
        this.role = role;
        this.status = "1";
        this.mailingList = "";
        this.lastActivity = "";
        this.diskQuota = 0;
        this.diskUsage = 0;
        this.buckets.add(new Bucket("default"));
        this.projects.add(new Project("default"));
    }

    public Account(String accountId, String accountName, String email, String password, String role, String status,
                   String mailingList, long diskQuota, long diskUsage, Session session, List<Session> oldSessions,
                   List<Bucket> buckets, String lastActivity, List<Credential> accounts, List<AnalysisPlugin> plugins,
                   List<Config> configs, List<Project> projects) {
        this.accountId = accountId;
        this.accountName = accountName;
        this.email = email;
        this.password = password;
        this.role = role;
        this.status = status;
        this.mailingList = mailingList;
        this.diskQuota = diskQuota;
        this.diskUsage = diskUsage;
        this.sessions.add(session);
        this.lastActivity = lastActivity;
        this.oldSessions = oldSessions;
        this.buckets = buckets;
        this.credentials = accounts;
        this.plugins = plugins;
        this.configs = configs;
        this.projects = projects;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getAccountName() {
        return accountName;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMailingList() {
        return mailingList;
    }

    public void setMailingList(String mailingList) {
        this.mailingList = mailingList;
    }

    public long getDiskQuota() {
        return diskQuota;
    }

    public void setDiskQuota(long diskQuota) {
        this.diskQuota = diskQuota;
    }

    public long getDiskUsage() {
        return diskUsage;
    }

    public String getLastActivity() {
        return lastActivity;
    }

    public void setLastActivity(String lastActivity) {
        this.lastActivity = lastActivity;
    }

    public void setDiskUsage(long diskUsage) {
        this.diskUsage = diskUsage;
    }

    public List<Session> getSessions() {
        return sessions;
    }

    public void setSessions(List<Session> sessions) {
        this.sessions = sessions;
    }

    public void addSession(Session session) {
        this.sessions.add(session);
    }

    public List<Session> getOldSessions() {
        return oldSessions;
    }

    public void setOldSessions(List<Session> oldSessions) {
        this.oldSessions = oldSessions;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<Bucket> projects) {
        this.buckets = projects;
    }

    public List<Credential> getAccounts() {
        return credentials;
    }

    public void setAccounts(List<Credential> accounts) {
        this.credentials = accounts;
    }

    public List<AnalysisPlugin> getPlugins() {
        return plugins;
    }

    public void setPlugins(List<AnalysisPlugin> plugins) {
        this.plugins = plugins;
    }

    public List<Config> getConfigs() {
        return configs;
    }

    public void setConfigs(List<Config> configs) {
        this.configs = configs;
    }

    public List<Project> getProjects() {
        return projects;
    }

    public void setProjects(List<Project> projects) {
        this.projects = projects;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

}

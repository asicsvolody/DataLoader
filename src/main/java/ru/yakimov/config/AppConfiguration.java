/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 * Класс для хранения конфигурации приложения
 */

package ru.yakimov.config;

import org.springframework.stereotype.Component;

public class AppConfiguration {

    private String hdfsHost;
    private String hdfsPort;

    private DBConfiguration workSchema;
    private String logTable;
    private String structTable;

    private String tempDir;
    private String jobsDir;
    private String logsDir;


    public String getHdfsHost() {
        return hdfsHost;
    }

    public String getHdfsPort() {
        return hdfsPort;
    }

    public String getTmpDir() {
        return tempDir;
    }

    public String getJobsDir() {
        return jobsDir;
    }

    public String getLogsDir() {
        return logsDir;
    }

    public DBConfiguration getWorkSchema() {
        return workSchema;
    }

    public String getLogTable() {
        return logTable;
    }

    public String getStructTable() {
        return structTable;
    }

    public void setHdfsHost(String hdfsHost) {
        this.hdfsHost = hdfsHost;
    }

    public void setHdfsPort(String hdfsPort) {
        this.hdfsPort = hdfsPort;
    }

    public void setTmpDir(String tempDir) {
        this.tempDir = tempDir;
    }

    public void setJobsDir(String jobsDir) {
        this.jobsDir = jobsDir;
    }

    public void setLogsDir(String logsDir) {
        this.logsDir = logsDir;
    }

    public void setWorkSchema(DBConfiguration workSchema) {
        this.workSchema = workSchema;
    }

    public void setLogTable(String logTable) {
        this.logTable = logTable;
    }

    public void setStructTable(String structTable) {
        this.structTable = structTable;
    }
}

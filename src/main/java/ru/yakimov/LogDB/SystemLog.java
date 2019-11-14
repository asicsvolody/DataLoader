/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.LogDB;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "SYSTEM_LOG")

public class SystemLog {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "SYSTEM_LOG_ID")
    private int id;

    @Column(name = "SYSTEM_LOG_JOB_NAME", nullable = false)
    private String jobName;

    @Column(name = "SYSTEM_LOG_ROOT_JOB")
    private String rootJob;

    @Column(name = "SYSTEM_LOG_MSG",nullable = false)
    private String msg;

    @Column(name = "SYSTEM_LOG_LEVEL", nullable = false)
    private String level;

    @Temporal(value = TemporalType.TIMESTAMP)
    @Column(name = "SYSTEM_LOG_DATE")
    private Date date;

    public SystemLog() {
    }

    public SystemLog(String jobName, String rootJob, String msg, Date dateTime, String level) {
        this.jobName = jobName;
        this.rootJob = rootJob;
        this.msg = msg;
        this.level = level;
        this.date = dateTime;
    }

    public int getId() {
        return id;
    }

    public String getJobName() {
        return jobName;
    }

    public String getRootJob() {
        return rootJob;
    }

    public String getMsg() {
        return msg;
    }

    public String getLevel() {
        return level;
    }

    public Date getDate() {
        return date;
    }
    public void setId(int id) {
        this.id = id;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setRootJob(String rootJob) {
        this.rootJob = rootJob;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public void setDate(Date dateTime) {
        this.date = dateTime;
    }

    @Override
    public String toString() {
        return "SystemLog{" +
                "id=" + id +
                ", jobName='" + jobName + '\'' +
                ", rootJob='" + rootJob + '\'' +
                ", msg='" + msg + '\'' +
                ", level='" + level + '\'' +
                ", date=" + date +
                '}';
    }
}

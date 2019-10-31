/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс контейнер для хранения конфингурации задания
 */

package ru.yakimov.config;

import java.util.ArrayList;
import java.util.List;

public class JobConfiguration{

    private String rootJobName;
    private String jobName;
    private String jobClass;
    private int stage;
    private List<String> dirFrom;
    private String dirTo;
    private List<String> partitions;
    private DBConfiguration dbConfiguration;

    public JobConfiguration(String rootJobName) {
        this.rootJobName = rootJobName;
    }

    public String getRootJobName() {
        return rootJobName;
    }

    public String getJobName() {
        return jobName;
    }

    public String getJobClass() {
        return jobClass;
    }

    public int getStage() {
        return stage;
    }

    public String[] getDirsFrom() {
        return dirFrom.toArray(new String[0]);
    }

    public String getDirTo() {
        return dirTo;
    }


    public String[] getPartitions() {
        return partitions.toArray(new String[0]);
    }

    public DBConfiguration getDbConfiguration() {
        return dbConfiguration;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public void setJobClass(String jobClass) {
        this.jobClass = jobClass;
    }

    public void setStage(int stage) {
        this.stage = stage;
    }

    public void setDirFrom(String dirFromLine) {
        if(this.dirFrom == null)
            this.dirFrom = new ArrayList<>();
        for (String dir : dirFromLine.split(",")) {
            this.dirFrom.add(dir.trim());
        }
    }

    public void setDirTo(String dirTo) {
        this.dirTo = dirTo;
    }

    public void setPartitions(String partitionLine) {
        if(partitions == null)
            partitions = new ArrayList<>();

        for (String partition : partitionLine.split(",")) {
            this.partitions.add(partition.trim());
        }
    }

    public void setDbConfiguration(DBConfiguration dbConfiguration) {
        this.dbConfiguration = dbConfiguration;
    }

}

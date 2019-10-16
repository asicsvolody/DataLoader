/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.config;


import java.util.ArrayList;
import java.util.List;

public class RootJobConfiguration {

    private String rootJobName;
    private List<JobConfiguration> jobConfigs;

    public RootJobConfiguration(String rootJobName) {
        this.rootJobName = rootJobName;
    }

    public String getRootJobName() {
        return rootJobName;
    }

    public List<JobConfiguration> getJobConfigurations() {
        return jobConfigs;
    }

    public void addJobConfiguration(JobConfiguration jobConfig) {
        if(jobConfigs == null)
            jobConfigs = new ArrayList<>();
        this.jobConfigs.add(jobConfig);
    }

}

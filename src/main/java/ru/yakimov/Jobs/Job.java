/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import ru.yakimov.config.JobConfiguration;

import java.util.concurrent.Callable;

public abstract class Job implements Callable<Integer> {

    JobConfiguration jobConfig;

    public void setJobConfig(JobConfiguration jobConfig) {
        this.jobConfig = jobConfig;
    }
}


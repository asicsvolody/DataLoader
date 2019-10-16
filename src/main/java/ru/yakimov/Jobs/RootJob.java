/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.Jobs;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.yakimov.Assets;
import ru.yakimov.JobContextConfiguration;
import ru.yakimov.config.JobConfiguration;
import ru.yakimov.config.RootJobConfiguration;
import ru.yakimov.MySqlDB.Log;

import java.util.*;
import java.util.List;
import java.util.concurrent.*;

public class RootJob implements Callable<Integer> {

    public static final Class CONTEXT_CLASS = JobContextConfiguration.class;


    private static final ApplicationContext context = new AnnotationConfigApplicationContext(CONTEXT_CLASS);

    private String rootJobName;

    private Map<Integer, ArrayList<Job>> jobsMap;

    public RootJob(RootJobConfiguration jobConfiguration) throws ClassNotFoundException {
        this.rootJobName = jobConfiguration.getRootJobName();
        this.jobsMap = jobConfigsToTreeMap(jobConfiguration.getJobConfigurations());
    }

    @Override
    public Integer call() throws Exception {
        int workRes = 0;

        if(jobsMap.isEmpty()){
            Log.writeRoot(rootJobName, "Sub jobs not found");
            return workRes;
        }


        for (Integer stage : jobsMap.keySet()) {
            Log.writeRoot(rootJobName, "Start stage: " + stage);
            ExecutorService service = Executors.newFixedThreadPool(6);


            Map<String, Future<Integer>> futuresMap = new HashMap<>();

            for (Job job : jobsMap.get(stage)) {
                futuresMap.put(job.jobConfig.getJobName(), service.submit(job));
                Log.writeRoot(rootJobName, "Start job: " + job.jobConfig.getJobName());
            }
            service.shutdown();

            Log.writeRoot(rootJobName, "Have run all jobs stage: "+ stage);


            service.awaitTermination(10, TimeUnit.HOURS);


            for (String jobName : futuresMap.keySet()) {
                if(futuresMap.get(jobName).get() != 0){
                    Log.writeRoot(rootJobName, "Error in stage: " + stage + "Job: "+ jobName, Log.Level.ERROR);
                    Log.writeRoot(Assets.MAIN_PROS, "Error in stage: "+ stage + " in " +rootJobName+" / "+ jobName );
                    return 1;
                }

            }
        }

        return workRes;
    }

    public Map<Integer, ArrayList<Job>> jobConfigsToTreeMap(List<JobConfiguration> jobConfigs) throws ClassNotFoundException {
        Map<Integer, ArrayList<Job>> resMap = new TreeMap<>();
        for (JobConfiguration jobConfig : jobConfigs) {
            int stage = jobConfig.getStage();
            Job job = null;
            try {
                job = (Job) context.getBean(Class.forName(jobConfig.getJobClass()));
            } catch (ClassNotFoundException e) {
                throw new ClassNotFoundException("There is't job class: "+jobConfig.getJobClass() );
            }
            job.setJobConfig(jobConfig);
            if(!resMap.containsKey(stage)){
                resMap.put(stage, new ArrayList<>());
            }
            resMap.get(stage).add(job);
        }
        return resMap;
    }


    public String getRootJobName() {
        return rootJobName;
    }
}



/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс статических методов создания корневых задач
 */

package ru.yakimov.utils;

import ru.yakimov.BootProcessMain;
import ru.yakimov.Jobs.RootJob;
import ru.yakimov.config.JobXmlLoader;
import ru.yakimov.config.RootJobConfiguration;
import ru.yakimov.LogDB.Log;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;

public class RootJobsCreator {

    /**
     * Метод создания массива корневых задач из директории
     *
     * @param jobsDir
     * @return
     * @throws SQLException
     */
    public  static RootJob[] getRootJobsFromDir(String jobsDir) throws SQLException {

        ArrayList <RootJob> resRootJobs = new ArrayList<>();

        File[] rootJobsFiles = new File(jobsDir).listFiles();

        if(rootJobsFiles == null)
            return null;

        for (File file : rootJobsFiles) {
            if(file.getPath().endsWith(".jxml")){
                Log.writeRoot(BootProcessMain.MAIN_PROS, "Read file: "+file.getPath());
                RootJob rootJob = getRootJobsFromFile(file);
                if(rootJob != null){
                    resRootJobs.add(rootJob);
                    Log.writeRoot(BootProcessMain.MAIN_PROS, "Create new rootJob:"+ rootJob.getRootJobName());
                }

            }
        }

        return resRootJobs.toArray(new RootJob[0]);
    }

    /**
     * Метод создания корневой задачи из файла
     *
     * @param file
     * @return
     * @throws SQLException
     */
    private static RootJob getRootJobsFromFile(File file) throws SQLException {
        RootJob rootJob = null;

        RootJobConfiguration config = JobXmlLoader.readConfJob(file);
        if(config == null) {
            Log.writeRoot(BootProcessMain.MAIN_PROS, "Wrong configuration from file: "+ file.getPath());
            return null;
        }
        Log.writeRoot(BootProcessMain.MAIN_PROS, "Create new root job configuration:"+ config.getRootJobName());
        try {
            rootJob = new RootJob(JobXmlLoader.readConfJob(file));

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            Log.writeSysException(BootProcessMain.MAIN_PROS, e);
        }

        return rootJob;
    }
}

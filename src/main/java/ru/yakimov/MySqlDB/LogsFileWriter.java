/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.MySqlDB;

import ru.yakimov.Assets;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class LogsFileWriter {

    public static final String JOB_NAME_COLUMN = "SYSTEM_LOG_JOB_NAME";
    public static final String ROOT_NAME_COLUMN = "SYSTEM_LOG_ROOT_JOB";



    public static void writeJobLog (String jobName) throws Exception {
        write(jobName, JOB_NAME_COLUMN);


    }

    public static void writeRootLog (String jobName) throws Exception {
        write(jobName, ROOT_NAME_COLUMN);
    }

    public static void write (String jobName, String column) throws Exception {
        File dirTo = new File(Assets.getInstance().getConf().getLogsDir());
        if(dirTo.mkdirs()){
            Log.writeRoot(Assets.MAIN_PROS, "Log dir to have created: "+dirTo.getPath());
        }
//        Log.writeRoot(Assets.MAIN_PROS, "Read logs from mysql");

        String sql = String.format("SELECT SYSTEM_LOG_MSG FROM SYSTEM_LOG WHERE %s = '%s'",column, jobName);
        List<String> logsArr = MySqlDb.getSqlResults(sql);


//        Log.writeRoot(Assets.MAIN_PROS, "Read logs from mysql"+ dirTo);

        try(FileWriter output = new FileWriter(new File(
                dirTo.toString()+Assets.SEPARATOR+jobName+".log"))){
            for (String s : logsArr) {
                output.write(s +"\n");
            }

        }catch (IOException e){
            e.printStackTrace();
            Log.writeRootException(Assets.MAIN_PROS, e);
        }
    }

}

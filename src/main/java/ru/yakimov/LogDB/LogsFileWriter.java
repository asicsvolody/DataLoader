/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс сгружает логи из таблицы в файлы
 */

package ru.yakimov.LogDB;

import ru.yakimov.BootProcessMain;
import ru.yakimov.config.AppConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class LogsFileWriter {

    public static final String JOB_NAME_COLUMN = "SYSTEM_LOG_JOB_NAME";
    public static final String ROOT_NAME_COLUMN = "SYSTEM_LOG_ROOT_JOB";


    public static void writeJobLog (String jobName) {
        write(JOB_NAME_COLUMN,jobName);
    }


    public static void writeRootLog (String jobName){
        write(ROOT_NAME_COLUMN,jobName);
    }



    public static void write (String column, String jobName){
        File dirTo = new File(BootProcessMain.CONTEXT.getBean(AppConfiguration.class).getLogsDir());
        if(dirTo.mkdirs()){
            Log.writeRoot(BootProcessMain.MAIN_PROS, "Log dir to have created: "+dirTo.getPath());
        }
        List<SystemLog> logsList = Log.getLogs(column, jobName);

        try(FileWriter output = new FileWriter(new File(
                dirTo.toString()+BootProcessMain.SEPARATOR+jobName+".log"))){
            for (SystemLog log : logsList) {
                output.write(log.getMsg() +"\n");
            }

        }catch (IOException e){
            e.printStackTrace();
            Log.writeRootException(BootProcessMain.MAIN_PROS, e);
        }
    }

}

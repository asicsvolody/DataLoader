/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс сгружает логи из таблицы в файлы
 */

package ru.yakimov.MySqlDB;

import ru.yakimov.BootProcessMain;
import ru.yakimov.config.AppConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Log {

    public static final String JOB_NAME_COLUMN = "SYSTEM_LOG_JOB_NAME";
    public static final String ROOT_NAME_COLUMN = "SYSTEM_LOG_ROOT_JOB";


    /**
     * Метод записи логов определённого задания
     *
     * @param jobName
     * @throws Exception
     */
    public static void writeJobLog (String jobName) throws Exception {
        write(JOB_NAME_COLUMN,jobName);
    }

    /**
     * Метод записи логов корневой задачи
     *
     * @param jobName
     * @throws Exception
     */
    public static void writeRootLog (String jobName) throws Exception {
        write(ROOT_NAME_COLUMN,jobName);
    }



    public static void write (String colum, String jobName) throws Exception {
        File dirTo = new File(BootProcessMain.CONTEXT.getBean(AppConfiguration.class).getLogsDir());
        if(dirTo.mkdirs()){
            Log.writeRoot(BootProcessMain.MAIN_PROS, "Log dir to have created: "+dirTo.getPath());
        }
        List<SystemLog> logsList = LogHib.getLogs(colum, jobName);

        try(FileWriter output = new FileWriter(new File(
                dirTo.toString()+BootProcessMain.SEPARATOR+jobName+".log"))){
            for (SystemLog log : logsList) {
                output.write(log.getMsg() +"\n");
            }

        }catch (IOException e){
            e.printStackTrace();
            Log.writeRootException(BootProcessMain.MAIN_PROS, e);
        }




//        File dirTo = new File(BootProcessMain.CONTEXT.getBean(AppConfiguration.class).getLogsDir());
//        if(dirTo.mkdirs()){
//            Log.writeRoot(BootProcessMain.MAIN_PROS, "Log dir to have created: "+dirTo.getPath());
//        }
//
//        String sql = String.format("SELECT SYSTEM_LOG_MSG FROM SYSTEM_LOG WHERE %s = '%s'",column, jobName);
//        List<String> logsArr = MySqlDb.getLogs(sql);
//
//
//        try(FileWriter output = new FileWriter(new File(
//                dirTo.toString()+BootProcessMain.SEPARATOR+jobName+".log"))){
//            for (String s : logsArr) {
//                output.write(s +"\n");
//            }
//
//        }catch (IOException e){
//            e.printStackTrace();
//            Log.writeRootException(BootProcessMain.MAIN_PROS, e);
//        }
    }

}

/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 *
 * Класс сгружает логи из таблицы в файлы
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


    /**
     * Метод записи логов определённого задания
     *
     * @param jobName
     * @throws Exception
     */
    public static void writeJobLog (String jobName) throws Exception {
        write(jobName, JOB_NAME_COLUMN);
    }

    /**
     * Метод записи логов корневой задачи
     *
     * @param jobName
     * @throws Exception
     */
    public static void writeRootLog (String jobName) throws Exception {
        write(jobName, ROOT_NAME_COLUMN);
    }


    /**
     * Запись логов определённого процесса
     *
     * @param jobName
     * @param column
     * @throws Exception
     */
    public static void write (String jobName, String column) throws Exception {
        File dirTo = new File(Assets.getInstance().getConf().getLogsDir());
        if(dirTo.mkdirs()){
            Log.writeRoot(Assets.MAIN_PROS, "Log dir to have created: "+dirTo.getPath());
        }

        String sql = String.format("SELECT SYSTEM_LOG_MSG FROM SYSTEM_LOG WHERE %s = '%s'",column, jobName);
        List<String> logsArr = MySqlDb.getSqlResults(sql);


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

/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 * Отображение таблицы Mysql SYSTEM_LOG и методы работы с ней
 * Таблица хранит логи работы системы
 */
package ru.yakimov.MySqlDB;

import ru.yakimov.config.JobConfiguration;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

public class Log {

    /**
     *  enum
     *  Константы уровней логирования
     */

    public enum Level{
        TRACE("TRACE")
        , DEBUG("DEBUG")
        , INFO("INFO")
        , WARNING("WARNING")
        , ERROR("ERROR")
        , FATAL("FATAL");

        public String levelName;

        Level(String levelName) {
            this.levelName = levelName;
        }

        public String getLevelName() {
            return levelName;
        }
    }


    public static synchronized void write(String jobName, String rootJob, String msg, Log.Level level,
                             boolean doThrow) throws SQLException {
        System.out.println(jobName+": "+msg);

        String sql = "INSERT INTO SYSTEM_LOG(SYSTEM_LOG_JOB_NAME,SYSTEM_LOG_ROOT_JOB,SYSTEM_LOG_MSG,SYSTEM_LOG_LEVEL,SYSTEM_LOG_DATA)" +
                " VALUES(?,?,?,?,?)";
        PreparedStatement ps = MySqlDb.getConnection().prepareStatement(sql);
        ps.setString(1, jobName);
        ps.setString(2,rootJob);
        ps.setString(3, msg.replaceAll("/n",""));
        ps.setString(4, level.getLevelName());
        ps.setString(5, String.valueOf(LocalDate.now()));
        ps.executeUpdate();
        ps.close();
        MySqlDb.getConnection().commit();

        if (doThrow) {
            MySqlDb.closeConnection();
            System.err.println(msg);
            System.exit(-1);
        }
    }


    public static void write(String jobName, String rootJob, String msg) throws SQLException {
        write(jobName, rootJob,  msg, Level.INFO, false);
    }

    public static synchronized void write(JobConfiguration config, String msg) throws SQLException {
        write(config.getJobName(), config.getRootJobName(), msg);
    }

    public static synchronized void write(JobConfiguration config, String msg, Log.Level level)
            throws SQLException {
        write(config.getJobName(), config.getRootJobName(), msg, level);
    }



    public static void write(String jobName, String rootJob, String msg, Log.Level level)
            throws SQLException {
        write(jobName, rootJob, msg, level, false);
    }

    public static void writeRoot(String jobName, String msg) throws SQLException {
        write(jobName, jobName,  msg, Level.INFO, false);
    }

    public static synchronized void writeException(String jobName, String rootJob, Level level,Exception e){
        try {
            write(jobName,rootJob,"WARRING ERROR!!!", level, false);
            write(jobName,rootJob, e.toString(), level, false);
            StackTraceElement[] stackTrace = e.getStackTrace();
            for (StackTraceElement element : stackTrace) {
                    write(jobName,rootJob, element.toString(), level, false);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static synchronized void writeSysException(String jobName, Exception e){
        writeException(jobName, jobName, Level.FATAL, e);

    }


    public static synchronized void writeRootException(String jobName,Exception e){
        writeException(jobName, jobName, Level.ERROR, e);
    }




    public static synchronized void writeException(JobConfiguration config,Exception e){
        writeException(config.getJobName(), config.getRootJobName(), Level.ERROR, e);
    }




    public static void writeRoot(String jobName, String msg, Log.Level level) throws SQLException {
        write(jobName, jobName, msg, level, false);
    }


    public static void raise(String jobName, String rootJob, String msg) throws SQLException {
        write(jobName, rootJob,  msg, Level.ERROR, true);
    }

    public static void raise(JobConfiguration config, String msg) throws SQLException {
        write(config.getJobName(), config.getRootJobName(),  msg, Level.ERROR, true);
    }

    public static void raiseRoot(String jobName, String msg) throws SQLException {
        write(jobName, jobName,  msg, Level.ERROR, true);
    }





}

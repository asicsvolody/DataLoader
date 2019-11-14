/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 * Отображение таблицы Mysql SYSTEM_LOG и методы работы с ней
 * Таблица хранит логи работы системы
 */
package ru.yakimov.LogDB;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.joda.time.DateTime;
import ru.yakimov.config.JobConfiguration;
import java.util.List;

public class Log {

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

    private static SessionFactory factory =  new Configuration().configure().buildSessionFactory();

    public static void closeConnection() {
        if (factory.isClosed()) {
            return;
        }
        factory.close();
    }

    public static synchronized void write(String jobName, String rootJob, String msg, Level level, boolean doThrow){
        System.out.println(jobName+": "+msg);

        Session session = factory.openSession();
        Transaction transaction = null;
        transaction = session.beginTransaction();
        SystemLog systemLog = new SystemLog(jobName,rootJob,msg, DateTime.now().toDate(),level.levelName);
        session.save(systemLog);
        transaction.commit();
        session.close();

        if (doThrow) {
            closeConnection();
            System.err.println(msg);
            System.exit(-1);
        }

    }

    public static List<SystemLog> getLogs(String column, String jobName){
        Session session = factory.openSession();
        Transaction transaction = null;

        transaction = session.beginTransaction();
        List<SystemLog> systemLogs = session.createQuery(String.format("FROM SystemLog WHERE %s=%s",jobName, column)).list();

        transaction.commit();
        session.close();

        return systemLogs;
    }

    public static void write(String jobName, String rootJob, String msg)  {
        write(jobName, rootJob,  msg, Level.INFO, false);
    }

    public static void write(JobConfiguration config, String msg){
        write(config.getJobName(), config.getRootJobName(), msg);
    }

    public static void write(JobConfiguration config, String msg, Level level){
        write(config.getJobName(), config.getRootJobName(), msg, level);
    }

    public static void write(String jobName, String rootJob, String msg, Level level){
        write(jobName, rootJob, msg, level, false);
    }

    public static void writeRoot(String jobName, String msg){
        write(jobName, jobName,  msg, Level.INFO, false);
    }

    public static synchronized void writeException(String jobName, String rootJob, Level level, Exception e){

        write(jobName,rootJob,"WARRING ERROR!!!", level, false);
        write(jobName,rootJob, e.toString(), level, false);
        StackTraceElement[] stackTrace = e.getStackTrace();

        for (StackTraceElement element : stackTrace) {
            write(jobName,rootJob, element.toString(), level, false);
        }

    }

    public static void writeSysException(String jobName, Exception e){
        writeException(jobName, jobName, Level.FATAL, e);

    }

    public static  void writeExceptionAndGet (JobConfiguration jConf, String exceptionMsg) throws Exception {
        Exception e = new Exception(exceptionMsg);
        writeException(jConf.getJobName(),jConf.getRootJobName(), Level.ERROR,e);
        throw e;
    }

    public static void writeRootException(String jobName,Exception e){
        writeException(jobName, jobName, Level.ERROR, e);
    }

    public static void writeException(JobConfiguration config,Exception e){
        writeException(config.getJobName(), config.getRootJobName(), Level.ERROR, e);
    }

    public static void writeRoot(String jobName, String msg, Level level){
        write(jobName, jobName, msg, level, false);
    }

    public static void raise(String jobName, String rootJob, String msg){
        write(jobName, rootJob,  msg, Level.ERROR, true);
    }

    public static void raise(JobConfiguration config, String msg){
        write(config.getJobName(), config.getRootJobName(),  msg, Level.ERROR, true);
    }

    public static void raiseRoot(String jobName, String msg){
        write(jobName, jobName,  msg, Level.ERROR, true);
    }












}

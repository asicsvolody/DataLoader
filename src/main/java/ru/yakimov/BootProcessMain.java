/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov;

import ru.yakimov.Jobs.RootJob;
import ru.yakimov.MySqlDB.Log;
import ru.yakimov.MySqlDB.LogsFileWriter;
import ru.yakimov.utils.RootJobsCreator;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class BootProcessMain {
    private RootJob[] rootJobs;


    public BootProcessMain() {
        try {

            rootJobs = RootJobsCreator.getRootJobsFromDir(Assets.getInstance().getConf().getJobsDir());


            if (rootJobs == null) {
                Log.writeRoot(Assets.MAIN_PROS, "Jobs not found");
                return;
            }

            ExecutorService service = Executors.newCachedThreadPool();

            Map<String, Future<Integer>> futureMap = new HashMap<>();

            Log.writeRoot(Assets.MAIN_PROS, "ExecutorService is ready");


            for (RootJob rootJob : rootJobs) {
                futureMap.put(rootJob.getRootJobName(), service.submit(rootJob));

                Log.writeRoot(Assets.MAIN_PROS, rootJob.getRootJobName()+"have been run");
            }

            Log.writeRoot(Assets.MAIN_PROS, "All jobs have been run");

            service.shutdown();

            service.awaitTermination(10, TimeUnit.HOURS);


            for (String rootJobName : futureMap.keySet()) {
                printFutureResults(rootJobName, futureMap.get(rootJobName));
            }
            Log.writeRoot(Assets.MAIN_PROS, "BootProsesMain has finished.");

            Log.writeRoot(Assets.MAIN_PROS, "Write logs files.");

            Log.writeRoot(Assets.MAIN_PROS, "Finish!!!");




        }catch (SQLException e){
            e.printStackTrace();
        }
        catch (Exception e){
            e.printStackTrace();
            Log.writeSysException(Assets.MAIN_PROS, e);
        }
        finally {

            try {
                for (RootJob rootJob : rootJobs) {
                    LogsFileWriter.writeRootLog(rootJob.getRootJobName());
                }
                LogsFileWriter.writeRootLog(Assets.MAIN_PROS);
            } catch (Exception e) {
                System.out.println("Logs not write");
                e.printStackTrace();
            }

            Assets.closeResources();

        }

    }

    private static void printFutureResults(String rootJobName, Future<Integer> future) throws ExecutionException, InterruptedException, SQLException {
        StringBuilder str = new StringBuilder(rootJobName);
        switch (future.get()){
            case 0:
                str.append(" completed successfully.");
                break;
            case 1:
                str.append(" completed with error");
                break;
        }

        System.out.println(str.toString());
        Log.writeRoot(Assets.MAIN_PROS, str.toString());
    }

    public static void main(String[] args) {
        new BootProcessMain();
    }
}

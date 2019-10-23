/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;

public class LoaderUtils {

    public static String[] getUsualCols(StructType type, String [] partition){
        ArrayList<String> resCols = new ArrayList<>();
        for (String field : type.fieldNames()) {
            if(!isPartition(field, partition)){
                resCols.add(field);
            }
        }
        return resCols.toArray(new String[0]);
    }

    public static boolean isPartition(String name, String[] partitions) {
        for (String partition : partitions) {
            if(name.toLowerCase().equals(partition.toLowerCase()))
                return true;
        }
        return false;
    }
}

/**
 * Created by IntelliJ Idea.
 * User: Якимов В.Н.
 * E-mail: yakimovvn@bk.ru
 */

package ru.yakimov.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

public class SparkUtils {

    public static StructType getNewStructType(StructType dataType, StructType newDataType) {

        Set<StructField> dataTypeFieldSet = new HashSet<>(Arrays.asList(dataType.fields()));
        List<StructField> newDataList = Arrays.asList(newDataType.fields());

        if(dataTypeFieldSet.containsAll(newDataList))
            return dataType;

        dataTypeFieldSet.addAll(newDataList);
        return new StructType(dataTypeFieldSet.toArray(new StructField[0]));
    }

    public static Column[] getUsingCols(Dataset<Row> dataOne, Dataset<Row>  dataTwo, StructType dataType) {

        List<Column> resCols = new ArrayList<>();
        for (StructField field : dataType.fields()) {
            if(dataOne.first().schema().contains(field)){
                resCols.add(dataOne.col(field.name()));
            }else{
                resCols.add(dataTwo.col(field.name()));
            }
        }
        return resCols.toArray(new Column[0]);
    }

    public static String[] getArrayWithColsAndNull(String[] mainCols, List<String> newDataList){
        getColumnNameOnly(mainCols);
        newDataList= getListColumnNameOnly(newDataList);

        for (int i = 0; i < mainCols.length ; i++) {
            if (!newDataList.contains(mainCols[i]))
                mainCols[i] = "null as "+ mainCols[i];
        }
        return mainCols;

    }

    private static String[] getColumnNameOnly(String[] colArr){
        for (int i = 0; i <colArr.length ; i++) {
            colArr[i] = colArr[i].split(" ")[0];
        }
        return colArr;
    }
    private static List<String> getListColumnNameOnly(List<String> colList){
        List<String> res = new ArrayList<>();
        for (String str : colList) {
            res.add(str.split(" ")[0]);
        }
        return res;
    }



    public static List<Column> getPrimaryColumns(Dataset<Row> data, ArrayList<String> primaryKeys) {
        List<Column> resList = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            resList.add(data.col(primaryKey));
        }
        return resList;
    }
}

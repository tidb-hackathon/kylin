package com.zhihu.sample;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Random;
import org.apache.flink.api.java.utils.ParameterTool;

public class SampleTextData {

  public static String CREATE_HIVE_TABLE_SQL = "CREATE TABLE `tidb_dim`\n"
      + "(\n"
      + "    `id`                 BIGINT,\n"
      + "    `sex`                STRING,\n"
      + "    `region`             STRING,\n"
      + "    `education`          STRING,\n"
      + "    `marriage`           STRING,\n"
      + "    `member`             STRING,\n"
      + "    `payment`            STRING,\n"
      + "    `focus`              STRING,\n"
      + "    `consumption_amount` BIGINT,\n"
      + "    `online_time`        BIGINT\n"
      + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;";

  public static String[][] dims = new String[][]{
      new String[]{"男", "女"},
      new String[]{"北京市", "上海市", "天津市", "重庆市", "河南省", "安徽省", "福建省",
          "甘肃省", "贵州省", "海南省", "河北省", "湖北省", "湖南省", "吉林省", "江苏省",
          "江西省", "辽宁省", "青海省", "山东省", "山西省", "陕西省", "四川省", "云南省",
          "浙江省", "台湾省", "广东省", "黑龙江省"},
      new String[]{"文盲", "小学", "初中", "高中", "本科", "硕士", "博士"},
      new String[]{"已婚", "未婚"},
      new String[]{"非会员", "月费会员", "季费会员", "年费会员"},
      new String[]{"微信", "支付宝", "银行卡", "信用卡"},
      new String[]{"手机", "电脑", "家居", "厨具", "图书", "其他"}
  };

  public static int measureCount = 2;

  public static int count() {
    return Arrays.stream(dims)
        .reduce(1, (integer, strings) -> integer * strings.length, (i, j) -> j);

  }

  public static void writeAsTextFile(OutputStream outputStream) {
    try (PrintWriter writer = new PrintWriter(outputStream)) {
      int id = 1;
      Random random = new Random();
      int[] index = new int[dims.length];
      String[] values = new String[dims.length];
      boolean reachedEnd = false;
      while (!reachedEnd) {
        for (int i = 0; i < values.length; i++) {
          values[i] = dims[i][index[i]];
        }
        // write values
        String data = String.join(",", values);
        // concat id and measures
        // write 1000 records
        for (int j = 1; j <= 100; j++) {
          String dataToWrite = String.format("%d,%s", id++, data);
          for (int i = 1; i <= measureCount; i++) {
            int randomValue = random.nextInt(100);
            dataToWrite = String.format("%s,%d", dataToWrite, randomValue);
          }
          writer.println(dataToWrite);
        }
        // add index
        reachedEnd = true;
        for (int i = 0; i < index.length; i++) {
          if (index[i] < dims[i].length - 1) {
            index[i] = index[i] + 1;
            for (int j = 0; j < i; j++) {
              index[j] = 0;
            }
            reachedEnd = false;
            break;
          }
        }
      }
    }
  }


  public static void main(String[] args) throws FileNotFoundException {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String path = parameterTool.getRequired("path");
    System.out.println("Start generate sample data...");
    writeAsTextFile(new FileOutputStream(path));
    System.out.println(
        "You could create hive table by following SQL: \n" + CREATE_HIVE_TABLE_SQL);
  }

}

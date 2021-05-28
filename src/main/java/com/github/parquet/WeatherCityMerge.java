package com.github.parquet;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-05-28 10:20
 * @description
 */
public class WeatherCityMerge {

    public static void main(String[] args) throws Exception{
        if(args.length != 3){
            System.err.println("args number error!");
            System.exit(1);
        }

        String csvFile = args[0];
        String reportCodeFile = args[1];
        String sinkDir = args[2];

        List<String> reportCodeLines = csvRead(reportCodeFile);
        List<String> rawCsvLines = csvRead(csvFile);

        Set<String> dates = new HashSet<>();
        for (String rawCsvLine : rawCsvLines) {
            String[] arr = rawCsvLine.split(",");
            String date = arr[1];
            dates.add(date);
        }

        Map<String,String> reportCodeMap = new HashMap<>();
        for (String reportCodeLine : reportCodeLines) {
            String[] items = reportCodeLine.split(",");
            String key = items[0];
            String value = items[1] + "," + items[2] + "," + items[3];
            reportCodeMap.put(key,value);
        }

        for (String date : dates) {
            String sinkFile = sinkDir + "/" + date + ".csv";

            FileWriter fr = new FileWriter(sinkFile);

            for (String rawCsvLine : rawCsvLines) {
                String[] arr = rawCsvLine.split(",");
                String rawDate = arr[1];
                if(rawDate.equals(date)){
                    String rawCityId = arr[0];

                    String cityId = rawCityId.replaceAll("CN","");

                    String rawArea = reportCodeMap.getOrDefault(cityId,"");
                    if(!rawArea.trim().isEmpty()){
                        String[] items = rawArea.split(",");
                        String adm1 = items[0];
                        String adm2 = items[1];
                        String adm3 = items[2];
                        String hour = arr[2];
                        String temp = arr[3];

                        StringBuilder sb = new StringBuilder();
                        sb.append(cityId);
                        sb.append(",");
                        sb.append(adm1);
                        sb.append(",");
                        sb.append(adm2);
                        sb.append(",");
                        sb.append(adm3);
                        sb.append(",");
                        sb.append(date);
                        sb.append(",");
                        sb.append(hour);
                        sb.append(",");
                        sb.append(temp);
                        sb.append(",");
                        sb.append(date);    // 分区列

                        fr.write(sb.toString());
                        fr.write("\n");
                    }
                }

            }

            fr.close();
        }
    }

    private static List<String> csvRead(String file){
        FileReader fileReader;
        BufferedReader br = null;
        List<String> lines = new ArrayList<>();

        try{
            fileReader = new FileReader(file);
            br = new BufferedReader(fileReader);
            String line = br.readLine();
            while(line != null){
                lines.add(line.replaceAll("\"",""));
                line = br.readLine();
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            if(br != null){
                try{
                    br.close();
                }catch (IOException ioe){
                    ioe.printStackTrace();
                }
            }
        }
        return lines;
    }

}

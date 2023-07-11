package org.example;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvValidationException;
import lombok.Data;



import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;


public class insertCems2023 {
    public static void  main(String[] args) {


        CSVReader csvReader = null;
        List<Kwh> Kwhs = null ;
        List<KwhC> KwhCs = null ;
        try {
//            csvReader = new CSVReader(new FileReader("/data/sample_kwh_20230711.csv"));
//
//            String[] line;
//            while ((line = csvReader.readNext()) != null) {
//                System.out.println(String.join(",", line));
//            }

            Kwhs = new CsvToBeanBuilder<Kwh>(new FileReader("/data/sample_kwh_20230711.csv"))
                    .withType(Kwh.class)
                    .build()
                    .parse();



        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
//        } catch (CsvValidationException e) {
//            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String token = System.getenv("INFLUX_TOKEN");
        String bucket = "CEMS2023";
        String org = "things";

        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", "ace2267","Dlwlsgh007!".toCharArray());
        WriteApiBlocking writeApi = client.getWriteApiBlocking();
       // Kwhs.forEach(kwh -> convertTime(kwh));

       // Kwhs.forEach(kwh -> System.out.println(kwh.getKwh()+", "+kwh.getId()+", "+kwh.getTime()));
      //  Kwhs.forEach(kwh -> writeApi.writeMeasurement(bucket, org, WritePrecision.NS, kwh));

        KwhCs = new ArrayList<>();
        for (Kwh kwh : Kwhs) {
            KwhC k2 = new KwhC();
            k2.setId(kwh.getId());
            k2.setKwh(kwh.getKwh());
            k2.setTime(convertTime(String.valueOf(kwh.getTime())));

            KwhCs.add(k2);
        }

        KwhCs.forEach(kwh -> System.out.println(kwh.getKwh()+", "+kwh.getId()+", "+kwh.getTime()));
        KwhCs.forEach(kwh -> writeApi.writeMeasurement(bucket, org, WritePrecision.NS, kwh));


        client.close();

    }

    public static Instant convertTime(String time){

        //String stringDate = "09:15:30 PM, Sun 10/09/2022";
        String pattern = "hh:mm:ss a, EEE M/d/uuuu";
        pattern = "yyyy-MM-dd H:m";
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern, Locale.UK);
        LocalDateTime localDateTime = LocalDateTime.parse(time , dateTimeFormatter);
        ZoneId zoneId = ZoneId.of("Asia/Seoul");
        ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
        Instant instant = zonedDateTime.toInstant();

        return instant;

    }

    @Measurement(name = "kwh")
    @Data
    public static class Kwh {


        @Column(tag = true)
        String id= "cems.test.001";

        @CsvBindByName
        @Column
        Double Kwh;

        @CsvBindByName
        @Column(timestamp = true)
        String time;
    }


    @Measurement(name = "kwh")
    @Data
    public static class KwhC {


        @Column(tag = true)
        String id= "cems.test.001";

        @Column
        Double Kwh;

        @Column(timestamp = true)
        Instant time;
    }


}
package org.example;

import java.time.Instant;
import java.util.List;

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

public class Main {
    public static void main(String[] args) {

        // You can generate an API token from the "API Tokens Tab" in the UI
        String token = System.getenv("INFLUX_TOKEN");
        String bucket = "NOAA_water_database";
        String org = "things";

        // InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());
        InfluxDBClient client = InfluxDBClientFactory.create("http://localhost:8086", "ace2267","Dlwlsgh007!".toCharArray());

        String data = "mem,host=host1 used_percent=23.43234543";

        WriteApiBlocking writeApi = client.getWriteApiBlocking();
        writeApi.writeRecord(bucket, org, WritePrecision.NS, data);

        Point point = Point
                .measurement("mem")
                .addTag("host", "host1")
                .addField("used_percent", 23.43234543)
                .time(Instant.now(), WritePrecision.NS);

        WriteApiBlocking writeApi2 = client.getWriteApiBlocking();
        writeApi2.writePoint(bucket, org, point);

        Mem mem = new Mem();
        mem.host = "host1";
        mem.used_percent = 23.43234543;
        mem.time = Instant.now();

        WriteApiBlocking writeApi3 = client.getWriteApiBlocking();
        writeApi3.writeMeasurement(bucket, org, WritePrecision.NS, mem);

        String query = "from(bucket: \"NOAA_water_database\") |> range(start: -1h)";
        List<FluxTable> tables = client.getQueryApi().query(query, org);

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                System.out.println(record.getValues().toString());
            }
        }



        client.close();

    }


    @Measurement(name = "mem")
    public static class Mem {
        @Column(tag = true)
        String host;
        @Column
        Double used_percent;
        @Column(timestamp = true)
        Instant time;
    }
}
package com.project.ingestion.service;


import com.google.common.primitives.Ints;
import com.kafka.DataPacketRequest;
import com.kafka.DataPacketResponse;
import com.kafka.IngestionServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;


@GrpcService
public class IngestionService extends IngestionServiceGrpc.IngestionServiceImplBase {
    Logger log= LoggerFactory.getLogger(this.getClass());
    @Value(value="${spring.message.topic.ingestion}")
    private String kafkaTopic;
    Runtime runtime=Runtime.getRuntime();
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;
    @Autowired
    RedisTemplate<String, String> redisTemplate;
    @Override
    public void ingestDataPacket(DataPacketRequest request, StreamObserver<DataPacketResponse> responseObserver)
    {
        log.info(String.valueOf(request));
        String version=request.getVersion();

        JSONObject dataRequest = new JSONObject();
        dataRequest.put("message",request);
        ArrayList<String> parsedData = new ArrayList<>();
        String patchId = request.getPatchId();
        String caseNumber = request.getCaseNumber();
        String deviceName = request.getDeviceName();
        String[] packet = request.getDataList().toArray(new String[0]);
        log.info(String.valueOf(packet.length));

        JSONObject replyMsg = new JSONObject();
        ArrayList<Long> ingestedTimeStamps=new ArrayList<>();
        JSONParser jsonParser = new JSONParser();
        JSONObject newObject;



        for (String s : packet) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(s);


                if (version.toLowerCase() =="v2") {

                    jsonObject.put("time",  jsonObject.get("recordTime"));
                    newObject=jsonObject;
                    String a = new String (String.valueOf(newObject));
                    jsonObject.clear();


                    jsonObject.put("extras",a);
                    a=String.valueOf(newObject.get("recorTime"));
                    jsonObject.put("time", a);
                    jsonObject.put("timeStamp", a);
                    newObject.clear();
                    log.info(String.valueOf(newObject));

                }
                if (jsonObject.containsKey("extras") && jsonObject.containsKey("time")) {
                    long v = Long.valueOf((String) jsonObject.get("time"));
                    ingestedTimeStamps.add(v);
                } else if (jsonObject.containsKey("temperatureData") && jsonObject.containsKey("recordedTime")) {
                    ingestedTimeStamps.add(Long.valueOf((String) jsonObject.get("recordedTime")));
                } else if (jsonObject.containsKey("timeStamp")) {
                    ingestedTimeStamps.add(Long.valueOf((String) jsonObject.get("timeStamp")));
                }
                parsedData.add(jsonObject.toJSONString());
                jsonObject.clear();

            } catch (Exception e) {
               log.error(String.valueOf(e));
//                System.out.println(e);
            }

        }
        log.info(String.valueOf(ingestedTimeStamps));
        dataRequest.replace("data",parsedData);
        log.info("message :"+dataRequest);
        //System.out.println(dataRequest);
        List<String> logTimeStamps = new ArrayList<>();
        replyMsg.put("ingestedTimeStamps",ingestedTimeStamps);
        for (Long timeStamp : ingestedTimeStamps) {
            long unixTimestamp = timeStamp;
            Instant instant = Instant.ofEpochSecond(unixTimestamp);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM dd h:mm:ss a");
            String formattedTimestamp = formatter.format(ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()));
            logTimeStamps.add(formattedTimestamp);
        }
      //log.info("jwdjdj"+logTimeStamps.toString());

        DateFormat dateFormat = new SimpleDateFormat("MMM dd h:mm:ss.SSS a");
        String formattedDate = dateFormat.format(new Date());
        String serverMsg = formattedDate + " case: " + caseNumber + ", patchId: " + patchId + ", deviceName: " + deviceName + ", - successfully ingested " + packet.length + " packet(s).";
        replyMsg.put("server msg",serverMsg);
//        System.out.println(replyMsg);
        log.info(String.valueOf(replyMsg));
        Long totalMemory = runtime.totalMemory();
        Long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory-freeMemory;
        double totalMemoryMB = (double) totalMemory / (1024*1024);
        double freeMemoryMB = (double) freeMemory / (1024*1024);
        double usedMemoryMB = (double) usedMemory / (1024*1024);
        System.out.println("TotM: " + totalMemoryMB + " MB"+"\tFreeM: " + freeMemoryMB + " MB"+"\tUsedM: " + usedMemoryMB + " MB");
        log.info("TotM: " + totalMemoryMB + " MB"+"\tFreeM: " + freeMemoryMB + " MB"+"\tUsedM: " + usedMemoryMB + " MB");

        kafkaTemplate.send(kafkaTopic, dataRequest.get("message").toString());
        dataRequest=null;
        parsedData=null;


        LocalDateTime now = LocalDateTime.now();
        String key = caseNumber + ":ingestion:" + now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        LocalDateTime startOfDay = now.truncatedTo(ChronoUnit.DAYS);
        long min = ChronoUnit.MINUTES.between(startOfDay, now);

        redisTemplate.opsForValue().set(caseNumber + "_" + patchId + "_lastIngestedTime", String.valueOf(System.currentTimeMillis()), 864000, TimeUnit.SECONDS);
        redisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create().incr(BitFieldSubCommands.BitFieldType.unsigned(16)).valueAt(min).by(packet.length));
        redisTemplate.expire(key,10, TimeUnit.DAYS);
       // log.info(String.valueOf(ingestedTimeStamps));
        DataPacketResponse dataPacketResponse =DataPacketResponse.newBuilder()
                .setServermsg(serverMsg).addAllIngestedTimeStamps(ingestedTimeStamps).build();
        responseObserver.onNext(dataPacketResponse);
        responseObserver.onCompleted();

    }
}

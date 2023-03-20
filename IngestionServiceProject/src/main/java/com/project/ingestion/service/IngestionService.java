package com.project.ingestion.service;


import com.kafka.DataPacketRequest;
import com.kafka.DataPacketResponse;
import com.kafka.IngestionServiceGrpc;



import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;


import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
//import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;


@GrpcService
public class IngestionService extends IngestionServiceGrpc.IngestionServiceImplBase {
    final String  topic="f1";
    Runtime runtime=Runtime.getRuntime();
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;
    @Autowired
    RedisTemplate<String, String> redisTemplate;
    @Override
    public void ingestDataPacket(DataPacketRequest request, StreamObserver<DataPacketResponse> responseObserver)
    {
        JSONObject dataRequest = new JSONObject();
        dataRequest.put("request",request);
//        dataRequest.put("version","v2");

        ArrayList<String> parsedData = new ArrayList<>();
        String patchId = request.getPatchId();
        String caseNumber = request.getCaseNumber();
        String deviceName = request.getDeviceName();
        String[] packet = request.getDataList().toArray(new String[0]);

//        String key = String.format("%s_%s_ingestion",caseNumber,patchId);
        ///String value="864000";
        JSONObject replyMsg = new JSONObject();
        ArrayList<String> ingestedTimeStamps=new ArrayList<>();
        JSONParser jsonParser = new JSONParser();
        JSONObject newObject;

        for (String s : packet) {
            try {
                JSONObject jsonObject = (JSONObject) jsonParser.parse(s);

                if (dataRequest.containsKey("version") && dataRequest.get("version").toString().toLowerCase() == "v2") {
                    jsonObject.put("time",  jsonObject.get("recordTime"));

//                    JSONObject newObject;
                   newObject=jsonObject;
//                    System.out.println(newObject);
//                    jsonObject.clear();
//                    Map<String,JSONObject> map =new HashMap<String,JSONObject>();
//                   map.put("extras", newObject);
//                    System.out.println(map);
//                    //System.out.println(jsonObject);
                  // jsonObject.put("extras", newObject);

                    //jsonObject.put("time", newObject.get("recordTime").toString());
                   //jsonObject.put("timeStamp", newObject.get("recordTime").toString());
////                    System.out.println(jsonObject);
                }
                if (jsonObject.containsKey("extras") && jsonObject.containsKey("time")) {
                    String v = (String) jsonObject.get("time");
                    ingestedTimeStamps.add(v);
                } else if (jsonObject.containsKey("temperatureData") && jsonObject.containsKey("recordedTime")) {
                    ingestedTimeStamps.add((String) jsonObject.get("recordedTime"));
                } else if (jsonObject.containsKey("timeStamp")) {
                    ingestedTimeStamps.add((String) jsonObject.get("timeStamp"));
                }
                parsedData.add(jsonObject.toJSONString());
            } catch (Exception e) {
                System.out.println(e);
            }

        }
        //System.out.println(parsedData);
        replyMsg.put("ingestedTimeStamps",ingestedTimeStamps);
        DateFormat dateFormat = new SimpleDateFormat("MMM dd h:mm:ss.SSS a");
        String formattedDate = dateFormat.format(new Date());
        String serverMsg = formattedDate + " case: " + caseNumber + ", patchId: " + patchId + ", deviceName: " + deviceName + ", - successfully ingested " + packet.length + " packet(s).";
        replyMsg.put("server msg",serverMsg);
//        System.out.println(replyMsg);
        Long totalMemory = runtime.totalMemory();
        Long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory-freeMemory;
        double totalMemoryMB = (double) totalMemory / (1024*1024);
        double freeMemoryMB = (double) freeMemory / (1024*1024);
        double usedMemoryMB = (double) usedMemory / (1024*1024);
        System.out.println("TotM: " + totalMemoryMB + " MB"+"\tFreeM: " + freeMemoryMB + " MB"+"\tUsedM: " + usedMemoryMB + " MB");



      kafkaTemplate.send(topic, String.valueOf(parsedData));
       // redisTemplate.opsForValue().get(patchId);
        LocalDateTime now = LocalDateTime.now();
        String key = caseNumber + ":ingestion:" + now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        LocalDateTime startOfDay = now.truncatedTo(ChronoUnit.DAYS);
        long min = ChronoUnit.MINUTES.between(startOfDay, now);

        redisTemplate.opsForValue().set(caseNumber + "_" + patchId + "_lastIngestedTime", String.valueOf(System.currentTimeMillis()), 864000, TimeUnit.SECONDS);

        redisTemplate.opsForValue().bitField(key, BitFieldSubCommands.create().incr(BitFieldSubCommands.BitFieldType.unsigned(16)).valueAt(min).by(packet.length));

        redisTemplate.expire(key,10, TimeUnit.DAYS);

        DataPacketResponse dataPacketResponse =DataPacketResponse.newBuilder()
                .setServermsg("server").build();
        responseObserver.onNext(dataPacketResponse);
        responseObserver.onCompleted();
  
//        System.gc();
    }
}

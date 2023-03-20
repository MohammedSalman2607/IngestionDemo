const path = require("path");
const protoLoader = require("@grpc/proto-loader");
const grpc = require("@grpc/grpc-js");
const numeral = require('numeral')
const proto = protoLoader.loadSync(path.join('/home/vigo/Desktop/IngestionServiceProject/src/main/proto/ingestion.proto'));
const definition = grpc.loadPackageDefinition(proto);
const _ = require('lodash');
const moment = require('moment')
//const jsonDataArray = require('./data.json')

// const newdata = JSON.stringify(data)

//console.log(JSON.parse(newdata));

// const URL = (global.env === 'local') ? '0.0.0.0:50064' :
//             (global.env === 'qa') ? 'vivalink.mvm2.qa.vigocare.com:50064' :
//             (global.env === 'staging') ? 'vivalink.mvm2.staging.vigocare.com:50064' :
//             (global.env === 'prod') ? 'vivalink2.mvm.vigocare.com:50064'
//             : 'vivalink.mvm2.dev.vigocare.com:50064';

var client =new definition.com.kafka.IngestionService('0.0.0.0:9090', grpc.credentials.createInsecure())
const patchId ="kjewfkef";
const caseNumber = "123";
const deviceName ="werrt"
// let packets = {
//    "caseNumber": caseNumber,
//    "patchId": patchId, 
//    "deviceName":deviceName, 
//    "data" : [JSON.stringify(data)]
// }
//console.log(packets);
const {rss,heapTotal,heapUsed} = process.memoryUsage()

let date = new Date();
console.log("time stamp ",date.toISOString(),"rss ",numeral(rss).format('0.0 ib'),'heapTotal ',numeral(heapTotal).format('0.0 ib'),' Heap used ',numeral(heapUsed).format('0.0 ib'));


//client.ingestDataPacket = (packets) => {
   //for(let i=1;i<=5;i++){
//    setInterval(hello,5000)
//   function hello(){
     
//      client.ingestDataPacket(packets, (error, response) => {
//    //console.log(i+"th packet");
//       //console.log(response)
//    })

// }
//}}



//}
// let packet = {
//    caseNumber: 'caseNumber', patchId: 'patchId', deviceName: 'VV330', data: data
// }
//ingestDataPacket(packet)
const fs  = require('fs');
const { exit } = require("process");

var fileData = fs.readFileSync('./1000sec.json');
let StringData = fileData.toString()
let jsonDataArray = JSON.parse(StringData)
//console.log(jsonDataArray.length);
let rowCnt = 0
let num=-1;
let startTime = Number(jsonDataArray[0].data.time)
    let diffTime = moment().valueOf() - startTime
    //console.log("vv330: diffTime: ", diffTime);
setInterval(function timer() {
   let sensorData = []
   for (let i = 0; i < 100; i++) {
       let data = toString(jsonDataArray[rowCnt].data)
       let sourceTs = data.time
       data.extras.time = '' + (Number(data.extras.time) + diffTime)
       data.time = '' + (Number(data.time) + diffTime)
       data.extras.receiveTime = '' + (Number(data.extras.receiveTime) + diffTime)

       //console.log("vv330: Source Timestamp: ", sourceTs, " New Timestamp: ", data.time)

       sensorData.push(JSON.stringify(data))
       rowCnt = rowCnt + 1
       if (rowCnt == jsonDataArray.length) {
          
           rowCnt = 0;
           //console.log("completely inegsted");
           // Add Min 4 Sec Gap while rolling over the file to avoid conflicts in analysis
      //      sensorData = sensorData.slice(0, 1)
      //      startTime = Number(jsonDataArray[0].data.time)
      //      diffTime = moment().add(4, 'seconds')
      //      let StringData = fileData.toString()
      //      let jsonDataArray = JSON.parse(StringData)
      //  console.log(StringData);
       //h.process(StringData)valueOf() - startTime
           // diffTime = moment().valueOf() - startTime
         //   console.log("vv330: diffTime: ", diffTime);
           break;
       }
   }
   //console.log(sensorData);
   let packet = {
       'caseNumber':caseNumber, 'patchId':patchId, deviceName: 'VV330', data: sensorData
   }
   //console.log(packet)
   client.ingestDataPacket(packet,(error,response)=>{
       console.log(`${num+1}: Ingested 100 packets`, caseNumber, patchId) 
        //console.log(response);
   })
   
   num=num+1;
}, 1000);

function toString(o) {
   Object.keys(o).forEach(k => {
       if (typeof o[k] === 'object') {
           return toString(o[k]);
       }
       o[k] = '' + o[k];
   });
   return o;
}

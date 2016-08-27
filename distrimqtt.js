/**
 * DistriMqtt
 * Distributed MQTT service in nodejs based on mosca and mqtt
 * @repository https://github.com/vladbabii/DistriMqtt
 */
const
     winston        = require('winston')
    ,fs             = require('fs')
    ,path           = require('path')
    ;

var Current = {};
var Config = {};
var L = {};
L.og=winston.log;
winston.remove(winston.transports.Console);
winston.add(winston.transports.Console, {
     colorize       : true
    ,timestamp      : function(){
           var date = new Date();

           var hour = date.getHours();
           hour = (hour < 10 ? "0" : "") + hour;

           var min  = date.getMinutes();
           min = (min < 10 ? "0" : "") + min;

           var sec  = date.getSeconds();
           sec = (sec < 10 ? "0" : "") + sec;

           var year = date.getFullYear();

           var month = date.getMonth() + 1;
           month = (month < 10 ? "0" : "") + month;

           var day  = date.getDate();
           day = (day < 10 ? "0" : "") + day;


           ms = Date.now().toString();
           ms = ms.substring( ms.length-4 );

           return year + ":" + month + ":" + day + ":" + hour + ":" + min + ":" + sec + ':' + ms + ' - ' + Current.Name;
       }
});
winston.level = 'debug';

function showHelp(){
    console.log('DistriMqtt v 0.0.1');
};

process.argv.forEach(function (val, index, array) {
    if(val.substring(0,1)=='-'){
        var eq;
        eq=val.indexOf('=');
        if(eq!=-1){
            var option;
            option = val.substring(1,eq);
            var value;
            value  = val.substring(eq+1);
            while(option.length>1 && option.substring(0,1)=='-'){
                option=option.substring(1);
            }
            switch(option){
                case 'level':
                    winston.level = value;
                    break;

                case 'help':
                    showHelp();
                    process.exit(0);
                    break;

                case 'name':
                    value=value.trim();
                    Current.Name=value;
                    L.og('info','Config',{NodeName:value});
                    break;

                case 'config':
                    value=value.trim();
                    ConfigFilePath=value;
                    L.og('info','Config',{ConfigFilePath:value});
                    break;

                default:
                    L.og('warn','Unknown option [',option,']');
                    break;
            }
        }
    }
});

if(typeof(Current.Name)=='undefined'){
    L.og('error','No application name given');
    process.exit(1);
}


/**
 * Config file path can be overriden with --config
 */
L.og('info','Reading config file');
if(typeof(ConfigFilePath)=='undefined') {
    ConfigFilePath = 'config/' + Current.Name + '.json';
}
try{
    ConfigData=fs.readFileSync(ConfigFilePath);
}catch(ectp){
    L.og('error','No configuration file ['+ConfigFilePath+'] could be read');
    process.exit(1);
}
try{
    Config = JSON.parse(ConfigData);
}catch(ectp){
    L.og('error','Configuration file could not be converted from json');
    process.exit(1);
}

try{
    const mosca = require('mosca');
}catch(this_exception){
    L.og('error','Mosca module not found');
    L.og('error',this_exception);
    process.exit(1);
}

L.og('info','Starting Peer Server');
try{
    MqttPeerServerSettings = {
        host: Config.mqttpeer.host
        ,port: Config.mqttpeer.port
    };
}catch(this_exception){
    L.og('error','Host or port missing in config file for mqttpeer');
    L.og('error',this_exception);
    process.exit(1);
}

function PeerServerReady(){
    L.og('info','Peer server ready on ',{tcp:MqttPeerServerSettings.host+':'+MqttPeerServerSettings.port});
}
function PeerClientPublished(packet,client){
    if(typeof(packet.topic)!='undefined' && packet.topic.substring(0,5)=='$SYS/'){
        return ;
    }
    info={};
    info.client_id=false;
    info.packet_id=packet.messageId;
    info.topic=packet.topic;
    info.payload=packet.payload.toString();
    if(typeof(client)!='undefined'){
        info.client_id = client.id;
    }

    if((info.client_id!="false" && info.client_id!=false) && info.topic=='cmd/sync'){
        startTime=parseInt(info.payload);
        setImmediate(function(StartOfTime,info){
            L.og('info','Resync storage with '+info.client_id+' since '+StartOfTime+'');
            var index;
            index=0;
            var resync = MqttServer.persistence.db.createReadStream();
            resync.on('data',function(data){
                data.type='put';
                try {
                    data.value.payload = data.value.payload.toString();
                    index++;
                    L.og('debug','Sending '+index+' to '+info.client_id);
                    tci=clientid=info.client_id.substring(Config.peers.idprefix.length);
                    var xi={
                         topic      :  Config.peers.prefix + tci + '/resync'
                        ,payload    :  JSON.stringify(data)
                    };
                    PeerServer.publish(xi);
                }catch(conversionexception){
                    L.og('error',conversionexception);
                }
            });
            resync.on('close',function(){
                L.og('debug','Resync with '+info.client_id+' completed');
            });
        },startTime,info);
        return ;
    }

    if(info.client_id!=false && info.topic.substring(0,10)=='will/peer/'){
        try {
            data = JSON.parse(info.payload);
        }catch(this_exception){
            L.og('error','Could not parse payload to json')
        }
        if(typeof(data)!='object'){
            return ;
        }
        L.og('info','PeerWillRecv '+info.client_id+'>',data);
        if(typeof(PeerWills[info.client_id])=='undefined'){
            PeerWills[info.client_id]=[];
            L.og('info','Initialized will storage for '+info.client_id);
        }
        var topic;
        topic=data.topic;
        if(data.retain=="true"){
            data.retain=true;
        }
        if(data.retain=="false"){
            data.retain=false;
        }
        delete(data.topic);
        PeerWills[info.client_id][topic]=data;
        return ;
    }

    if(info.topic.substring(0,10)=='live/peer/') {
        setImmediate(function(peerID){
            SyncStorage(info.topic.substring(10),info.payload,false);
        },this.PeerIndex);
        return ;
    }
    if(info.topic.substring(0,11)=='store/peer/') {
        setImmediate(function(peerID){
            SyncStorage(info.topic.substring(11),info.payload,true);
        },this.PeerIndex);
        return ;
    }

    if(info.client_id==false){
        return ;
    }
    L.og('info','Peer>',info);
}
function PeerClientConnected(client) {
    L.og('info', 'Peer+',{client_id:client.id});

    cid=client.id.substring(Config.peers.idprefix.length);
    for(i in CurrentWills){
        setImmediate(function(xcid,xi){
            PeerConnectionSendAWill(xcid,CurrentWills[xi]);
        },cid,i);
    }

}
function PeerClientDisconnected(client) {
    L.og('info', 'Peer-',{client_id:client.id});

    if(typeof(PeerWills[client.id])!='undefined') {
        L.og('info','Dumping will storage for '+client.id);
        for(topic in PeerWills[client.id]){
            item = {
                  topic     : topic
                , payload   : PeerWills[client.id][topic].payload
                , qos       : PeerWills[client.id][topic].qos
                , retain    : PeerWills[client.id][topic].retain
            };
            MqttServer.publish(item);
        }
        delete(PeerWills[client.id]);
    }

}
var PeerServer = new mosca.Server(MqttPeerServerSettings);
PeerServer.on('ready'               ,PeerServerReady);
PeerServer.on('published'           ,PeerClientPublished);
PeerServer.on('clientConnected'     ,PeerClientConnected);
PeerServer.on('clientDisconnected'  ,PeerClientDisconnected);

const mqtt = require('mqtt');

PeerConfig={};
PeerConnection={};
PeerWills={};
CurrentWills={};
CurrentWillsDirty=false;
PeerLastSync={};  /* TODO : save lasy sync with a peer */
function ConnectPeers(){
    for(i in Config.peerlist){
        if(Config.peerlist[i]!=Current.Name) {
            setImmediate(function(peer){
                L.og('info', 'Trying to load config for peer ' + Config.peerlist[peer]);
                fs.readFile('config/'+Config.peerlist[peer]+'.json',function(err,data){
                    if(err){
                       L.og('warn','Could not read file for peer '+Config.peerlist[peer]);
                       return ;
                    }
                    try{
                        PeerConfig[Config.peerlist[peer]]=JSON.parse(data);
                    }catch(this_exception){
                        delete(PeerConfig[Config.peerlist[peer]]);
                    }
                    if(typeof(PeerConfig[Config.peerlist[peer]])=='object'){
                        setImmediate(function(){
                           ConfigurePeerConnection(Config.peerlist[peer]);
                        });
                    }else{
                        L.og('error','Could not convert to json the config for peer '+Config.peerlist[peer]);
                    }
                });
            },i);
        }
    }
}

function ConfigurePeerConnection(index){
    PeerConnection[index] = mqtt.connect('mqtt://'+PeerConfig[index].mqttpeer.host+":"+PeerConfig[index].mqttpeer.port,{
         clientId           : PeerConfig[index].peers.idprefix+Current.Name
        ,keepalive          : 3
        ,reconnectPeriod    : 5*1000
        ,connectTimeout     : 10*1000
        ,will: {
            topic: PeerConfig[index].peers.prefix+Current.Name+'/online'
            ,payload: "0"
            ,qos: 2
            ,retain: true
        }
    });
    PeerConnection[index].PeerIndex=index;
    PeerConnection[index].AskForResync = function(){
        L.og('info','Asking for sync data from '+index);
        this.publish('cmd/sync','0');
    };
    PeerConnection[index].on('connect',function(){
        L.og('info','RPeer+ '+index+', listening for resync on '+PeerConfig[index].peers.prefix+Current.Name+'/resync');
        this.subscribe(PeerConfig[index].peers.prefix+Current.Name+'/resync');
        this.publish(PeerConfig[index].peers.prefix+Current.Name+'/online',"1",{qos:2,retain:true});
        setImmediate(function(con){
            con.AskForResync();
        },PeerConnection[index]);
    });
    PeerConnection[index].on('disconnect',function(){
        L.og('info','RPeer- '+this.PeerIndex);
    });
    PeerConnection[index].on('end',function(){
        L.og('info','RPeerEnd '+this.PeerIndex);
    });
    PeerConnection[index].on('error',function(){
        L.og('RPeerError '+this.PeerIndex);
    });
    PeerConnection[index].on('message',function(topic,message) {

        /* resync from remote peer */
        if(topic==PeerConfig[this.PeerIndex].peers.prefix+Current.Name+'/resync'){
            setImmediate(function(peerID,xmessage){
                SyncStorage(peerID,xmessage.toString(),true);
            },this.PeerIndex,message);
            return ;
        }

        L.og('info','RPeer-'+this.PeerIndex+' > ',topic,message.toString());
    });
}

function PeerConnectionSendToall(info){
    subtopic='store/';
    if(info.retain==false){
        subtopic='live/';
    }
    str=JSON.stringify(info);
    for(index in PeerConnection){
        setImmediate(function(i,suffix){
            PeerConnection[i].publish(suffix+PeerConfig[i].peers.prefix+Current.Name,str);
            L.og('debug','PeerSent '+i,str);
        },index,subtopic)
    }
}

function PeerConnectionSendToallWill(info){
    subtopic='will/';
    str=JSON.stringify(info);
    for(index in PeerConnection){
        setImmediate(function(i,suffix){
            PeerConnection[i].publish(suffix+PeerConfig[i].peers.prefix+Current.Name,str);
            L.og('debug','PeerWillSent '+i,str);
        },index,subtopic);
    }
}
function PeerConnectionSendAWill(id,will){
    subtopic='will/';
    setImmediate(function(i,suffix,xwill){
        str=JSON.stringify(xwill);
        PeerConnection[i].publish(suffix+PeerConfig[i].peers.prefix+Current.Name,str);
        L.og('debug','PeerWillSent '+i,str);
    },index,subtopic,will);
}


NextSetTs=[];

function SyncStoragePublish(jdata){
    if(jdata.value.retain==true) {
        NextSetTs[jdata.value.topic + '|' + jdata.value.payload + '|' + jdata.value.qos + '|' + jdata.value.retain] = jdata.value.ts;
    }
    MqttServer.publish({
        topic     : jdata.value.topic
        , payload   : jdata.value.payload
        , qos       : jdata.value.qos
        , retain    : jdata.value.retain
        , ts        : jdata.value.ts
    });
    if(jdata.value.retain==true) {
        setImmediate(function (delkey) {
            setTimeout(function () {
                delete(NextSetTs[delkey]);
            }, 5000);
        }, jdata.value.topic + jdata.value.payload + jdata.value.qos + jdata.value.retain);
    }else{

        /**
         *  Delete retained topics that are no longer retained
         */
        MqttServer.persistence.db.get('!retained!'+jdata.value.topic,function(err,value) {
            if(typeof(err)!='undefined' && err==null && typeof(value.topic)!='undefined') {
                L.og('info','Deleting prior retained information at '+value.topic);
                MqttServer.persistence.db.del('!retained!'+value.topic,function(err) {
                    StorageDirty=true;
                });
            }
        });

    }
}

/** @TODO : Sync Storage **/
function SyncStorage(peer,data,doStore){
    L.og('info','Resync from '+peer+' with doStore=',doStore,' data is ',data);
    try {
        jdata = JSON.parse(data);
    }catch(this_exception){
        jdata=false;
    }

    if(
        typeof(jdata.value)=='undefined'
        && typeof(jdata.topic)!='undefined'
        && typeof(jdata.payload)!='undefined'
        && typeof(jdata.qos)!='undefined'
        && typeof(jdata.retain)!='undefined'
        && typeof(jdata.ts)!='undefined'
    ){
        jdata = {
            key     : jdata.topic
          , value   : jdata
        };
    }

    if(
        typeof(jdata)=='object'
        && typeof(jdata.key)!='undefined'
        && typeof(jdata.value.topic)!=false
        && typeof(jdata.value.payload)!=false
        && typeof(jdata.value.qos)!=false
        && typeof(jdata.value.retain)!=false
        && typeof(jdata.value.ts)!=false
    ) {
        MqttServer.persistence.db.get('!retained!'+jdata.key,function(err,value){

            if(typeof(err)!='undefined' && err!=null){
                setImmediate(function(somedata,per){
                    SyncStoragePublish(somedata);
                    L.og('debug','Resync '+somedata.value.topic+' from '+per+' was resolved as new data');
                },jdata,peer);
                return ;
            }

            if(jdata.value.ts < value.payload.ts){
                L.og('info','Resync '+jdata.value.topic+' from '+peer+' was resolved as older, ignoring');
                return ;
            }

            if(
                   jdata.value.payload.toString() != value.payload.toString()
                || jdata.value.qos != value.qos
                || jdata.value.retain != value.retain
            ){
                setImmediate(function(somedata,per) {
                    SyncStoragePublish(somedata);
                    L.og('info', 'Resync ' + somedata.value.topic + ' from ' + per + ' was resolved as newer data, with publish');
                },jdata,peer);
                return ;
            }

            if(
                jdata.value.payload.toString() == value.payload.toString()
                && jdata.value.qos == value.qos
                && jdata.value.retain == value.retain
            ) {
                L.og('info','Resync '+jdata.value.topic+' from '+peer+' was resolved as ~identical');
                return ;
            }

            L.og('info','Resync '+jdata.value.topic+' from '+peer+' was resolved as UNKNOWN');
        });
    }
}

function GetConfigStorageDelay(){
    var delay=1000;
    if(typeof(Config.storage.delay)=='numeric'){
        delay=Config.storage.delay;
    }
    if(
           typeof(Config.storage.delay_min)!='undefined'
        && typeof(Config.storage.delay_max)!='undefined'
    ) {
        delay = Math.floor(
                    Math.random() * (Config.storage.delay_max - Config.storage.delay_min) + Config.storage.delay_min
                    );
    }
    return delay;
}

StorageDirty=false;
StorageDumpFilePath='storage/'+Current.Name+'.ldb.json';
StorageDumpFilePathOld='storage/'+Current.Name+'.ldb.old.json';

function MqttServerDBSave(cb){
    var rs = MqttServer.persistence.db.createReadStream();
    var datadump=[];
    rs.on('data',function(data){
        delete(data.type);
        try {
            data.value.payload = data.value.payload.toString();
        }catch(conversionexception){
            L.og('error',conversionexception);
        }

        /**
         * If there is a will for this topic, then we should save the file to prepare for restart
         * The timestamp will be null, so any peer with newer data will be able to override it without issues
         * Since we want to write as small of a data as possible, we delete timestamp instead of nulling
         */
        for(j in CurrentWills){
            if(CurrentWills[j].topic == data.value.topic){
                data.value.payload  = CurrentWills[j].payload;
                data.value.qos      = CurrentWills[j].qos;
                data.value.retain   = CurrentWills[j].retain;
                /* Write optimization: Don't write ts if it should be null */
                delete(data.value.ts);
            }
        }

        /**
         * check retain if it has been overriden by a will
         */
        if(data.value.retain==true){
            /* Write optimization: retained messages always have retain=true */
            delete(data.value.retain);
            datadump.push(
                data
            );
        }
    });
    rs.on('close',function(){
        StorageDirty=false;

        if(datadump.length==0){
            /** Don't bother optimizing / encoding if we have no data **/
            fs.unlink(StorageDumpFilePathOld,function(){
                fs.rename(StorageDumpFilePath, StorageDumpFilePathOld, function() {
                    fs.writeFile(StorageDumpFilePath, '[]', "utf8", function () {
                        L.og('info', 'Done writing empty leveldb to file');
                        if (typeof(cb) !== 'undefined') {
                            cb();
                        }
                    });
                });
            });
            return ;
        }

        /**
         * Write optimization: get minimum ts (except undefined) and write it first in the file
         * it should save a couple of characters in the end
         */
        var min;
        min=false;
        for(i in datadump){
            if(typeof(datadump[i].value.ts)=='number' && datadump[i].value.ts==0) {
                delete(datadump[i]['value']['ts']);
            }
            if(typeof(datadump[i].value.ts)!='undefined' && (min==false || datadump[i].value.ts<min)){
                min=datadump[i].value.ts;
            }
            if(typeof(datadump[i].value.qos)!='undefined' && datadump[i].value.qos==0){
                delete(datadump[i].value.qos);
            }
            if(
                typeof(datadump[i].key)=='string'
                && typeof(datadump[i].value.topic)=='string'
                && datadump[i].key == '!retained!'+datadump[i].value.topic
            ){
                delete(datadump[i].value.topic);
                datadump[i].key=datadump[i].key.substring(10);
            };
        }
        for(i in datadump){
            if(typeof(datadump[i].value.ts)=='number'){
                datadump[i].value.ts-=min;
            }
        }
        if(min==false){ min=0; }
        datadump.unshift({
            saveoptions   : true
            , mints         : min
        });

        fs.unlink(StorageDumpFilePathOld,function(){
            fs.rename(StorageDumpFilePath, StorageDumpFilePathOld, function() {
                fs.writeFile(StorageDumpFilePath, JSON.stringify(datadump), "utf8", function () {
                    L.og('info', 'Done writing leveldb to file');
                    if (typeof(cb) !== 'undefined') {
                        cb();
                    }
                });
            });
        });
    });
}

function MqttServerDBLoad(){
    try {
        var jsraw = JSON.parse(fs.readFileSync(StorageDumpFilePath, 'utf8'));
    }catch(ecpt){
        L.og('error','Could not convert file '+StorageDumpFilePath+' to json, trying to load old file');
        L.og('error',JSON.stringify(ecpt));
        try{
            var jsraw = JSON.parse(fs.readFileSync(StorageDumpFilePathOld, 'utf8'));
        }catch(ecpt){
            L.og('error','Could not convert file '+StorageDumpFilePathOld+' to json, abandoning load');
            L.og('error',JSON.stringify(ecpt));
            return ;
        }
    }

    /* Some write optimizations undo-ing to get back the same data structure as before save optimizations */
    /* TODO : add a flag for save optimization */

    var opts={
        mints: 0
    };

    if(typeof(jsraw[0].saveoptions)!='undefined'){
        loaded = jsraw.shift();
        if(typeof(loaded.mints)!='undefined'){
            opts.mints = loaded.mints;
        }
    }
    for(i in jsraw){
        jsraw[i].type='put';
        if(typeof(jsraw[i].value.topic)!='string'){
            jsraw[i].value.topic = jsraw[i].key;
            jsraw[i].key = '!retained!'+jsraw[i].key;
        }
        if(typeof(jsraw[i].value.qos)=='undefined'){
            jsraw[i].value.qos=0;
        }
        if(typeof(jsraw[i].value.retain)=='undefined'){
            jsraw[i].value.retain=true;
        }
        if(typeof(jsraw[i].value.ts)=='undefined'){
            jsraw[i].value.ts=0;
        }else{
            jsraw[i].value.ts+=opts.mints;
        }
    }
    MqttServer.persistence.db.batch(jsraw);
    L.og('debug','Loaded '+jsraw.length+' items from file to memory');
 }

StorageSaveTimer=setTimeout(StorageSave,GetConfigStorageDelay());

function StorageSave(){
    if(StorageDirty==false){
        StorageSaveTimer = setTimeout(StorageSave,GetConfigStorageDelay());
        return ;
    }
    L.og('info','Saving memory storage to file');
    setTimeout(function(){
        MqttServerDBSave(function(){
            StorageDirty=false;
            StorageSaveInterval = setTimeout(StorageSave,GetConfigStorageDelay());
        });
    },GetConfigStorageDelay());
 }

L.og('info','Starting Mqtt Server');
try{
    MqttServerSettings = {
         host: Config.mqtt.host
        ,port: Config.mqtt.port
        ,http: {
             host: Config.http.host
            ,port: Config.http.port
        }
        ,persistence : {
            factory : mosca.persistence.Memory
        }
    };
}catch(this_exception){
    L.og('error','Host or port missing in config file for mqtt');
    L.og('error',this_exception);
    process.exit(1);
}

function MqttServerReady(){
    /**
     * Inject timestamp into the packet being saved to use later for solving packet comparison
     * after network-split heals and on resync
     *
     * Also mark storage for saving with StorageDirty=true
     **/
    MqttServer.persistence._original_storeRetained=MqttServer.persistence.storeRetained;
    MqttServer.persistence.storeRetained=function(packet,cb){

        /**
         * message id is not needed.. yet?
         * lower storage requirements
         */
        delete(packet.messageId);

        /**
         * Payload is useful as string
         */
        try {
            packet.payload = packet.payload.toString();
        }catch(conversion_error){
            L.og('error','Conversion error ',coversion_error);
        }

        /**
         * If packet to be saved does not have ts, then look up the preset TS if it exists
         * if not set current timestamp
         */
        if(typeof(packet.ts)=='undefined') {
            key = packet.topic +'|'+ packet.payload +'|'+ packet.qos +'|'+ packet.retain;
            if(typeof(NextSetTs[key])!='undefined'){
                packet.ts=NextSetTs[key];
                delete(NextSetTs[key]);
            }else{
                packet.ts = Date.now();
            }
        }

        StorageDirty=true;
        return MqttServer.persistence._original_storeRetained(packet,cb);
    };

    L.og('info','Mqtt server ready on ',{tcp:MqttServerSettings.host+':'+MqttServerSettings.port,http:MqttServerSettings.http.host+':'+MqttServerSettings.http.port});
    L.og('info','Loading saved messages into memory');
    MqttServerDBLoad();
    setImmediate(function(){
       ConnectPeers();
    });
}

function MqttClientPublished(packet,client){
    if(typeof(packet.topic)!='undefined' && packet.topic.substring(0,5)=='$SYS/'){
        return ;
    }
    /**
     * @TODO remove info and L.og(...,'info') or use flag to dump information about received packets
     */
    info={};
    info.client_id  = false;
    info.packet_id  = packet.messageId;
    info.topic      = packet.topic;
    info.payload    = packet.payload.toString();
    info.qos        = packet.qos;
    info.retain     = packet.retain;


    /**
     * If client is not set then it's published by this broker and this broker should know when to broadcast and when
     * not to so, skip it
     */
    /* @TODO : setImmediate */
    if(typeof(client)!='undefined' && client!=false && info.retain==false){
        info.client_id = client.id;
        info.ts = Date.now();
        setImmediate(function(data){
            PeerConnectionSendToall(data);
        },info);


        /**
         *  Delete retained topics that are no longer retained
         */
         MqttServer.persistence.db.get('!retained!'+info.topic,function(err,value) {
            if(typeof(err)!='undefined' && err==null && typeof(value.topic)!='undefined') {
                L.og('info','Deleting prior retained information at '+info.topic);
                MqttServer.persistence.db.del('!retained!'+value.topic,function(err) {
                    StorageDirty=true;
                });
            }
         });
    }

    /**
     * If it's retained, then it's already in memory, and we should get the item from there before sending it
     * so it has the same timestamp; this means a bit of delay before sending but less sync traffic in the end
     * and data should match as much as possible
     */
    /* @TODO : setImmediate */
    if(typeof(client)!='undefined' && client!=false && info.retain==true){
        MqttServer.persistence.db.get('!retained!'+info.topic,function(err,value){
            if(typeof(err)=='undefined' || err==null){
                try {
                    value.payload  = value.payload.toString();
                }catch(conversionerror){ }
                PeerConnectionSendToall({
                     key    : '!retained!'+value.topic
                    ,value: value
                });
            }else {
                PeerConnectionSendToall(info);
            }
        });
    }
    L.og('verbose','Client>',info);
}

function MqttClientConnected(client) {
    L.og('info', 'Client+',{client_id:client.id});
    if(typeof(client.will)!='undefined'){
        willInfo={
            topic: client.will.topic
            ,payload: client.will.payload.toString()
            ,qos: client.will.qos
            ,retain: client.will.retain
            ,clientid: client.id
            ,ts : Date.now()
        };
        L.og('info','ClientWill '+client.id,willInfo);
        if(
            typeof(client.will)=='object'
            && typeof(client.will.topic)!='undefined'
            && typeof(client.will.payload)!='undefined'
        ){
            CurrentWills[client.id]=willInfo;
            CurrentWillsDirty=true;
            PeerConnectionSendToallWill(willInfo);
        }
    }

}
function MqttClientDisconnected(client) {
    delete(CurrentWills[client.id]);
    L.og('info', 'Client-',{client_id:client.id});
}
var MqttServer = new mosca.Server(MqttServerSettings);
MqttServer.on('ready'               ,MqttServerReady);
MqttServer.on('published'           ,MqttClientPublished);
MqttServer.on('clientConnected'     ,MqttClientConnected);
MqttServer.on('clientDisconnected'  ,MqttClientDisconnected);

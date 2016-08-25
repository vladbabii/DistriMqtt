const
     winston        = require('winston')
    ,fs             = require('fs')
    ,path           = require('path')
    ;

var Current = {};
var Config = {};
var L = {};
L.og=winston.log;
winston.level = 'debug';

process.argv.forEach(function (val, index, array) {
    if(val.substring(0,1)=='-'){
        var eq;
        eq=val.indexOf('=');
        if(eq!=-1){
            var option;
            option = val.substring(1,eq);
            var value;
            value  = val.substring(eq+1);
            switch(option){
                case 'name':
                    value=value.trim();
                    Current.Name=value;
                    L.og('info','Config',{NodeName:value});
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


L.og('info','Reading config file');
ConfigFilePath='config/'+Current.Name+'.json';
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
            clientid=info.client_id.substring(Config.peers.idprefix.length);
            for(i in Storage){
                L.og('debug','Sending '+i+' to '+info.client_id);
                if(Storage[i].ts > StartOfTime) {
                    setImmediate(function (tci,index) {
                        send=Storage[index];
                        send.topic=index;
                        PeerServer.publish({
                             topic  : Config.peers.prefix + tci + '/resync'
                            ,payload: JSON.stringify(send)
                        });
                    }, clientid,i);
                }
            }
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
            if(item.retain==true){
                Storage[topic]={
                      payload   : PeerWills[client.id][topic].payload
                    , qos       : PeerWills[client.id][topic].qos
                    , ts        : Date.now()
                    , client_id : "will"
                };
                console.log("-----");
                console.log(Storage[topic]);
                console.log("-----");
                StorageDirty=true;
            }
            //console.log(item);
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
            setImmediate(function(peerID){
                SyncStorage(peerID,message.toString(),true);
            },this.PeerIndex);
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
        },index,subtopic)
    }
}

function SyncStorage(peer,data,doStore){
    L.og('info','Resync from '+peer+' with doStore=',doStore,' data is ',data);
    try {
        jdata = JSON.parse(data);
    }catch(this_exception){
        jdata=false;
    }
    if(typeof(jdata)=='object') {
        topic=jdata.topic;
        delete(jdata.topic);

        if(doStore==false){
            setImmediate(function(t,j) {
                MqttServer.publish({
                    topic: topic
                    , payload: jdata.payload
                    , qos: jdata.qos
                });
            },topic,jdata);
            if(typeof(Storage[topic])!='undefined'){
                delete(Storage[topic]);
                StorageDirty=true;
            }
            L.og('debug','Resync '+jdata.packet_id+' from '+peer+' was resolved as live');
            return ;
        }

        if(typeof(Storage[topic])=='undefined'){
            Storage[topic]=jdata;
            StorageDirty=true;
            L.og('debug','Resync '+jdata.packet_id+' from '+peer+' was resolved as new data');
            //setImmediate(function(){
               MqttServerRepublish(topic);
            //});
            return ;
        }

        if(Storage[topic].ts > jdata.ts){
            L.og('debug','Resync '+jdata.packet_id+' from '+peer+' had older ts, ignoring currentts='+Storage[topic].ts+' incomingts='+jdata.ts);
            return ;
        }

        var fields=["payload","qos","ts","client_id","packet_id"];
        var diff=[];
        var needRepublish=false;
        for(i in fields){
            if(Storage[topic][fields[i]] != jdata[fields[i]]){
                diff.push(fields[i]);
                if(
                    needRepublish == false
                    && (
                        fields[i]== "payload"
                        || fields[i]== "qos"
                    )
                ){
                 needRepublish=true;
                }
            }
        }
        if(diff.length>0){
            Storage[topic]=jdata;
            StorageDirty=true;
            if(needRepublish==false){
                L.og('debug','Resync '+jdata.packet_id+' from '+peer+' was partial newer, not republishing');
                return ;
            }
        }
        if(needRepublish){
            setImmediate(function(){
                MqttServerRepublish(topic);
            });
            L.og('debug','Resync '+jdata.packet_id+' from '+peer+' was much newer, republishing');
            return ;
        }

        L.og('debug','Resync '+jdata.packet_id+' from '+peer+' was resolved as old data, ignoring');
    }
}


StorageDirty=false;
StorageFilePath='storage/'+Current.Name+'.json';
try {
    Storage = JSON.parse(fs.readFileSync(StorageFilePath, 'utf8'));
}catch(ectp){
    Storage= {};
}
StorageSaveTimer=setTimeout(StorageSave,Config.storage.delay);
function StorageSave(){
    if(StorageDirty==false){
        StorageSaveTimer = setTimeout(StorageSave,Config.storage.delay);
        return ;
    }
    L.og('info','Saving storage to file');
    fs.writeFile(StorageFilePath,JSON.stringify(Storage),"utf8",function(){
        StorageDirty=false;
        StorageSaveInterval = setTimeout(StorageSave,Config.storage.delay);
    });
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
    L.og('info','Mqtt server ready on ',{tcp:MqttServerSettings.host+':'+MqttServerSettings.port,http:MqttServerSettings.http.host+':'+MqttServerSettings.http.port});
    L.og('info','Loading saved messages into memory');
    //setImmediate(function(){
       MqttServerLoadStorage();
    //});
    setImmediate(function(){
       ConnectPeers();
    });
}
function MqttServerLoadStorage(){
    counter=0;
    for(i in Storage){
        counter++;
        item = {
              topic     : i
            , payload   : Storage[i].payload
            , qos       : Storage[i].qos
            , retain    : true
        };
        setImmediate(function(itm){
            MqttServer.publish(itm);
        },item);
    }
    L.og('info','Loaded '+counter+' retained topics');
    /*console.log(Storage);*/
}
function MqttServerSaveToStorage(info){
    L.og('info','Saving to storage',{packet_id:info.packet_id,ts:info.ts});
    topic=info.topic;
    delete(info.topic);
    delete(info.retain);
    Storage[topic]=info;
    StorageDirty=true;
    /*console.log(MqttServer.persistence.db);*/
}
function MqttClientPublished(packet,client){
    /*if(typeof(client)=='undefined' || typeof(client.id)=='undefined'){
        console.log('nc',packet);
        console.log('nc',client);
        return ;
    }*/
    if(typeof(packet.topic)!='undefined' && packet.topic.substring(0,5)=='$SYS/'){
        //console.log('sy',packet);
        //console.log('sy',client);
        return ;
    }
    info={};
    info.client_id  = false;
    info.packet_id  = packet.messageId;
    info.topic      = packet.topic;
    info.payload    = packet.payload.toString();
    info.qos        = packet.qos;
    info.retain     = packet.retain;

    if(typeof(client)!='undefined'){
        setImmediate(function(data){
            PeerConnectionSendToall(data);
        },info);
    }

    if(info.retain==true){
        if(typeof(client)!='undefined') {
            setImmediate(function (data, cl) {
                data.ts = ts = Date.now();
                MqttServerSaveToStorage(data);
            }, info, client);
        }
    }else{
        if(typeof(Storage[info.topic])!='undefined'){
            delete(Storage[info.topic]);
            StorageDirty=true;
        }
    }
    if(typeof(client)!='undefined'){
        info.client_id = client.id;
    }
    L.og('info','Client>',info);
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


function MqttServerRepublish(topic){
    L.og('debug','Republishing '+topic);
    MqttServer.publish({
         topic      : topic
        ,payload    : Storage[topic].payload
        ,qos        : Storage[topic].qos
        ,retain     : true
    });
}

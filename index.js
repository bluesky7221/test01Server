
var io = require('socket.io')({
	transports: ['websocket'],
});
//rabbitMQ 사용
var amqp = require('amqplib/callback_api');
//토픽 방식을 사용
var ex = "topic";
var mongoose = require("mongoose");

//연결부
//서버가 닫히거나 여는데 실패하면 재연결
var count=0; //user 카운터
var amqpConn = null; //연결된 amqp를 전역으로 사용하여 publish, comsumer에서 채널 만들 때 사용
var User;
//amqp 연결 및 socket.io 연결
function start() {
  //mongoose DB 연결
  mongoose.Promise = global.Promise; //몽구스 mpromise 오류 메시지 고치는 프로마이즈 추가
  mongoose.connect("");
  var db = mongoose.connection;

  db.once("open", function(){
    console.log("DB connected");
  });
  db.on("error", function(err){
    console.log("DB ERROR :", err);
  });

  //defind scheme
  var userSchema = mongoose.Schema({
    id: String,
    stage: Number,
    revive: Boolean,
    playmin: Number,
    playsecond: Number,
    donation: Boolean,
		ranking: Number
  });
  //create model mongodb collection & schema

  try {
    User = mongoose.model('users');
  } catch (error) {
    User = mongoose.model('users', userSchema);
  }
  //amqp.createConnection({ host: 'localhost',port: 5673, login: 'test01', password: '20100772', vhost: '/'});
  amqp.connect('amqp://localhost', function(err, conn) {
    if (err) { //amqp.connect 예외처리 -> 재시작
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function(err) { //conn 예외처리
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() { //닫혔을 때 예외처리 -> 재시작
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });

    console.log("[AMQP] connected");
    amqpConn = conn;

    //socket.io 연결
    io.on('connection', function(socket){
      console.log('user connected: ', socket.id);
      //console.log(count, " user connecting !!");
      count++;
      //클라이언트가 접속 해제시
      socket.on('disconnect', function(){
        console.log('user disconnected: ', socket.id);
        //console.log(count, " user connecting !!");
        count--;
      });
			socket.on('want first register', function(who) {
				console.log("want first register -" + who.id);
				//현재 게임에서 얻은 랭킹 정보 받은 후
        var id = who.id;
        var revive = true;
        var stage = 0;
        var playmin = 0;
        var playsecond = 0.0;

        //저장된 정보가 없으면 저장
        User.findOne({'id':id}, function(err,user) {
          if(err) {
            console.log(err);
            return;
          }
          if (user === null)
          {
						var new_user = new User({'id':id, 'stage':stage, 'revive': revive, 'playmin': playmin, 'playsecond': playsecond, 'donation': false, 'ranking': 1});
            new_user.save(function(err,silence){
              if(err)
              {
                console.log(err);
                return;
              }
            });
          }
				});
      });

      socket.on('want ranking info', function(who) {
				console.log("want ranking info -" + who.id + who.stage + who.revive + who.min + who.second);
        var new_obj = {
          "socketID":socket.id,
          "id":who.id,
          "stage":who.stage,
          "revive":who.revive,
          "playmin":who.min,
          "playsecond":who.second
        };
				//console.log(new_obj);
        //메시지 발행
        publish(ex, "want ranking info", new Buffer.from(JSON.stringify(new_obj)));
				//console.log("publish");
      });

      socket.on('want donation', function(who) {
				console.log("want donation -" + who.id);
        //해당 아이디 데이터베이스 불러와서 과금bool true로 변환
        User.findOne({'id':who.id}, function(err,user) {
        if(err) {
        console.log(err);
        return;
        }
				//console.log(user.id + user.donation);

        user.donation = true;
        user.save(function(err,silence){
        if(err)
        {
          console.log(err);
          return;
        }
        });
        });
      });

      socket.on('isDonation', function(who) {
				console.log("isDonation -" + who.id);
        //해당 아이디 데이터베이스 불러와서 과금bool return
				User.findOne({'id':who.id}, function(err,user) {
        if(err) {
        console.log(err);
        return;
        }
				var new_donation = false;
				if (String(user.donation) == "true")
        {
          new_donation = true;
        }
				var new_json = { 'donation' : new_donation};
				//console.log(new_json);
        socket.emit('here donation info', new_json);
        });
      });
    });

    //port 80
    io.attach(80);

    whenConnected();
  });
}

//발행 -> exchange에서 binding해서 queues 생성
function whenConnected() {
  startPublisher();
  startWorker();
}

var pubChannel = null;
var offlinePubQueue = [];
function startPublisher() {
  //createConfirmChannel이란 confirmation mode를 사용하는 채널을 만드는것
  //확인 모드는 서버에 의해 acked or nacked 된 발행 메시지를 필요 -> 처리됨을 나타냄
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });

    pubChannel = ch;
    while (true) {
      //offlinePubQueue는 앱이 offline이면 메시지를 보낼 수 없는 내부적 큐
      //큐에 메시지가 더해진다면 앱이 큐를 확인
      var m = offlinePubQueue.shift();
      if (!m) break;
      publish(m[0], m[1], m[2]);
    }
  });
}

//publish msg
//만약 연결이 끊키면 연결됬을 때 큐에 저장해놨던 메시지들을 다 표시
function publish(exchange, routingKey, content) {
  try {
    //토픽으로 exchange 지정
    pubChannel.assertExchange(exchange, 'topic', {durable: true});
    //발행
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
                       function(err, ok) {
                         if (err) {
                           console.error("[AMQP] publish", err);
                           offlinePubQueue.push([exchange, routingKey, content]);
                           pubChannel.connection.close();
                         }
                       });
  } catch (e) {
    console.error("[AMQP] publish", e.message);
    offlinePubQueue.push([exchange, routingKey, content]);
  }
}

//worker는 메시지를 보내기를 성공했을 때만 work
function startWorker() {
  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });
    ch.prefetch(10);
    //exchange 설정
    ch.assertExchange(ex, 'topic', {durable: true});
    //큐 생성
    ch.assertQueue('', { exclusive: false }, function(err, q) {
      if (closeOnErr(err)) return;
      ch.bindQueue(q.queue, ex, "want ranking info");
      //consumer set up
      ch.consume(q.queue, function(ranking) {
				//console.log("consume");
        //현재 게임에서 얻은 랭킹 정보 받은 후
        var obj = JSON.parse(ranking.content.toString());
        var socketID = obj.socketID;
        var id = obj.id;
        var revive = true;
        if (String(obj.revive) == "false")
        {
          revive = false;
        }
        var stage = Number(obj.stage);
        var playmin = Number(obj.playmin);
        var playsecond = Number(obj.playsecond);
        //console.log(socketID + '//' + id + '//' + stage + '//' + revive + '//' + playmin + '//' + playsecond);
        //예전 기록보다 높으면 덮어씌우기
        User.findOne({'id':id}, function(err,user) {
					//console.log(user);
          if(err) {
            console.log(err);
            return;
          }
            var beforeRevive = true;
            if (String(user.revive) == "false")
            {
              beforeRevive = false;
            }
            if (stage > Number(user.stage))
            {
              user.stage = stage;
              user.revive = revive;
              user.playmin = playmin;
              user.playsecond = playsecond;
            }
            else if (stage == Number(user.stage))
            {
              if (revive === false && beforeRevive === true)
              {
                user.revive = revive;
                user.playmin = playmin;
                user.playsecond = playsecond;
              }
              else if (revive == beforeRevive)
              {
                if (playmin < Number(user.playmin))
                {
                  user.playmin = playmin;
                  user.playsecond = playsecond;
                }
                else if (playmin == Number(user.playmin))
                {
                  if (playsecond < Number(user.playsecond))
                  {
                    user.playsecond = playsecond;
                  }
                }
              }
            }
						//console.log("before save");
            user.save(function(err,silence){
              if(err)
              {
                console.log(err);
                return;
              }
							//18명 정보 보내주기
							User.find({}, {_id: 0, donation:0, ranking:0}, function(err,data) {
								//console.log(data);
								//플레이어 정보 및 90명 정보 보내주기
								for(var i=0;i<data.length;i++)
								{
									io.to(socketID).emit('here ranking info', data[i]);
									//console.log(data[i]);
								}
								//io.to(socketID).emit('here ranking info', JSON.parse(JSON.stringify(data)));
							}).limit(18).sort({stage:-1, revive:1, playmin:1, playsecond:1});

							var tempCount = 1;
							//해당 플레이어의 랭킹 구하기
							User.find({}, {_id: 0, donation:0, ranking:0}, function(err,data) {
                for(var i=0;i<data.length;i++)
                {
									if(user.id === data[i].id)
									{
										break;
									}
                  tempCount++;
									//console.log(tempCount);
                }
								//랭킹 추가한 유저 데이터 만들기
								var new_revive = true;
				        if (String(user.revive) == "false")
				        {
				          new_revive = false;
				        }
				        var new_stage = Number(user.stage);
				        var new_playmin = Number(user.playmin);
				        var new_playsecond = Number(user.playsecond);
								var new_donation = false;

								var new_user = new User({'id':user.id, 'stage':new_stage, 'revive': new_revive, 'playmin': new_playmin, 'playsecond': new_playsecond, 'donation': new_donation, 'ranking': tempCount});
	              io.to(socketID).emit('here your info', new_user);
								//console.log(new_user);
              }).sort({stage:-1, revive:1, playmin:1, playsecond:1});
            });
        });
      }, { noAck: true });
      console.log("want ranking info Worker is started");
    });
  });
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

start(); //실행 !!!!!!!!!!!!!!!!!

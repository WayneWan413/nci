var express = require('express');
var redis = require("redis");
var router = express.Router();

client = redis.createClient();

/* GET users listing. */
router.get('/', function(req, res) {
  res.send('nci web service.');
});


router.get('/topics', function(req, res) {
  client.get('topicwords',function(err,data){
  res.charset='utf-8';
  res.writeHead(200, {'Content-Type': 'application/json'});
  res.end(data);
  });
});

router.get('/topics/:id', function(req, res) {
  client.get('topic'+req.params.id,function(err,data){
  res.charset='utf-8';
  res.writeHead(200, {'Content-Type': 'application/json'});
  res.end(data);  
});
});

router.get('/news/:id', function(req, res) {
  client.get('news',function(err,data){
  res.charset='utf-8';
  res.writeHead(200, {'Content-Type': 'application/json'});
  res.end(data);
  });
});

router.get('/concept', function(req, res) {
  client.get('concept',function(err,data){
  res.charset='utf-8';
  res.writeHead(200, {'Content-Type': 'application/json'});
  res.end(data);
        });
});

router.get('/concept/:id', function(req, res) {
  client.get('concept'+req.params.id,function(err,data){
  res.charset='utf-8';
  res.writeHead(200, {'Content-Type': 'application/json'});
  res.end(data);
	});
});


module.exports = router;

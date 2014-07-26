var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res) {
  res.render('index', { title: 'NCI' });
});

router.get('/news', function(req, res) {
	res.render('news/newsview', { title: 'News View' ,user:req.user});
});

router.get('/concept', function(req, res) {
	res.render('concept/conceptview', { title: 'Concept View' ,user:req.user});
});

router.get('/invest', function(req, res) {
	res.render('invest/investview', { title: 'Invest View' ,user:req.user});
});

router.get('/topics', function(req, res) {
	res.render('concept/topicview', { title: 'Topics View' ,user:req.user});
});

router.get('/topics/:name', function(req, res) {
	res.render('concept/onetopic', { title: req.params.name, user:req.user });
});

module.exports = router;

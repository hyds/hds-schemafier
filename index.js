var stream = require('stream');
var util = require('util');
var mapping = require('./config/mapping.json');
var fs = require('fs');

module.exports = {
    hydstra : Hydstra,
    nrmgwdb : NRMGWDB
} 

// node v0.10+ use native Transform, else polyfill
var Transform = stream.Transform;

function Hydstra(options) {
  // allow use without new
  if (!(this instanceof Hydstra)) {
    return new Hydstra(options);
  }

  // init Transform
  Transform.call(this, options);
}

util.inherits(Hydstra, Transform);

Hydstra.prototype._transform = function (buf, enc, cb) {
    
    var line = JSON.parse(buf.toString().replace(/;$/,""));

    var mastdict = line._return.rows;
    var tables = [];
    
    for (table in mastdict){ if (!mastdict.hasOwnProperty(table)){ continue; }
    	tables.push(table);

 		var file = './data/'+table +'.js';
 		var schema = {};
 		for (fieldnumber in table){

 			if (!table.hasOwnProperty(fieldnumber)){continue;} 		
 			
 			for (fieldname in fieldname){
	 			if (!fieldname.hasOwnProperty(fieldname)){continue;}

	 			schema.fieldname = 	mapping.fldtype[fieldname.fldtype];

 			}
 		}

		var fileText = 	"module.exports = mongoose.model('"+table+"', "+table+"Schema); var mongoose = require('mongoose'),"+table+"Schema = mongoose.Schema({"+JSON.stringify(schema)+"},{collection:'"+table+"'};";

		fs.writeFile(file,fileText,function(err){
			if (err) throw err;
			console.log("saved ["+file+"]");
		});
 	}
    
    this.push(tables.toString(), 'utf8');

    cb();
};


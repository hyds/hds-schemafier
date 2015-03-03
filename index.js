var stream = require('stream');
var util = require('util');
var mapping = require('./config/mapping.json');
var fs = require('fs');

module.exports = {
    hydstra : Hydstra
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

    var line = JSON.parse(buf.toString().replace(/;$/g,""));
    var tables = [];
    var ret;

    // return key not consistent from Hydstra webservice between agencies!!!
    // It's an outrage sir!!!
    for (objKey in line){
      if (!line.hasOwnProperty(objKey)){ continue; }
      switch (objKey){
        case 'return':
         ret = 'return';
         break;
        case '_return':
         ret = '_return';
         break;
      }
    }

    var retrn = line[ret];
    var mastdict = retrn.rows;

    for (table in mastdict){
      if (!mastdict.hasOwnProperty(table)){ continue; }
      var tableDefinition = mastdict[table];
      tables.push(table);
      var schemaFile = './data/'+table +'.js';
      var schema = {};

      var definitionFile = './data/'+table +'.json';
      fs.writeFile(definitionFile,JSON.stringify(tableDefinition),function(err){
        if (err) throw err;
        console.log("saved ["+definitionFile+"]");
      });

      for (fieldnumber in tableDefinition){

        if (!tableDefinition.hasOwnProperty(fieldnumber)){continue;}
        var field = tableDefinition[fieldnumber];

        for (fieldname in field ){
          if (!field.hasOwnProperty(fieldname)){continue;}
          var fieldDefinition = field[fieldname];

          var fldtype = fieldDefinition.fldtype;
          fldtype.toLowerCase();
          console.log("fieldname [",fieldname,"]");
          console.log("fldtype ["+fldtype+"]");
  	 			var schemaType = {};
          schemaType[type] = mapping.fldtype[fldtype];
          schemaType[key] = fieldDefinition[keyfld];

          schemaType[uppercase] = fieldDefinition.uppercase.toLowerCase();

          schema[fieldname] = schemaType;


   			}
   		}


      var fileText = 	"module.exports = mongoose.model('"+table+"', "+table+"Schema); var mongoose = require('mongoose'),"+table+"Schema = mongoose.Schema("+JSON.stringify(schema)+",{collection:'"+table+"'};";
      fs.writeFile(schemaFile,fileText,function(err){
        if (err) throw err;
        console.log("saved ["+schemaFile+"]");
      });


      //console.log("saved ["+table+"]");

 	  }


    this.push(tables.toString(), 'utf8');

    cb();
};


var stream = require('stream');
var util = require('util');
var mapping = require('./config/mapping.json');
var schemaTemplate = require('./config/template.json');
var fs = require('fs');

var through = require('through2');
var duplexer = require('duplexer');
var split = require('split');

/*
module.exports = {
    hydstra : Hydstra
}
*/

module.exports = function(){
  var tables = [];
  return through(function write(data, _, next) {
    try { var line = JSON.parse(data.toString().replace(/;$/g,"")) }  
    catch (err){ return this.emit('error',err) }
    console.log(line);
    var mastdict = fixReturn(line);
    //data.tables = writeSchemas(schemaGen(mastdict));
    tables = writeSchemas(schemaGen(mastdict));
    next();
  },
  function end(cb){
    this.push(tables.toString(), 'utf8');
    this.queue(null);
    cb();
  })
}


function Hydstra(data){
  var input = through(write,end);
  //return duplexer(input,data);

  var tables = [];

  function write(buf, enc, next){
    //this.queue(buf);
    var line = JSON.parse(buf.toString().replace(/;$/g,""));
    var mastdict = fixReturn(line);
    //data.tables = writeSchemas(schemaGen(mastdict));
    tables = writeSchemas(schemaGen(mastdict));
    next();
  }

  function end(cb){
    this.push(tables.toString(), 'utf8');
    //this.queue(null);
    cb();
  }

}




// node v0.10+ use native Transform, else polyfill
var Transform = stream.Transform;

function Hydstra2(options) {
  // allow use without new
  if (!(this instanceof Hydstra)) {
    return new Hydstra(options);
  }

  // init Transform
  Transform.call(this, options);
}

util.inherits(Hydstra2, Transform);

Hydstra2.prototype._transform = function (buf, enc, cb) {

    /*
    fs.writeFile('./data/dump.json',buf.toString(), function(err){
      if (err) throw err;
      console.log('saved\n');
    });
    */

    var tables = [];

    var line = JSON.parse(buf.toString().replace(/;$/g,""));

    var mastdict = fixReturn(line);
    tables = writeSchemas(schemaGen(mastdict));
    this.push(tables.toString(), 'utf8');

    cb();
};



function schemaGen(mastdict){
  var schemas = {};

  for (table in mastdict){
    if (!mastdict.hasOwnProperty(table)){ continue; }
    var tableDefinition = mastdict[table];

    var lcTable = table.toLowerCase();
    var ucfTable = lcTable.charAt(0).toUpperCase() + lcTable.substr(1);

    schemas[lcTable] = processTableDefinition(tableDefinition);
    schemas[lcTable].collection = ucfTable;

    if ( 'undefined' !== typeof (mapping.dataModel[lcTable]) ){

      console.log("table [" ,lcTable,"], model [",mapping.dataModel[lcTable],"]");
      var parent = mapping.dataModel[lcTable];
      var ucfParent = parent.charAt(0).toUpperCase() + parent.substr(1);
      schemas[lcTable]['parent'] = ucfParent;
    }



    for (field in schemaTemplate ){
      if (!schemaTemplate.hasOwnProperty(field)){continue;}
      schemas[lcTable][field] = schemaTemplate[field];
    }

  }
  return schemas;
}

function processTableDefinition (tableDefinition) {
  var tableDef = {};
  for (fieldnumber in tableDefinition){

    if (!tableDefinition.hasOwnProperty(fieldnumber)){continue;}
    var field = tableDefinition[fieldnumber];

    for (fieldname in field ){
      if (!field.hasOwnProperty(fieldname)){continue;}
      var schemaType = {};
      var fieldDefinition = field[fieldname];
      var lcFieldname = fieldname.toLowerCase();
      var fldtype = fieldDefinition.fldtype.toUpperCase();
      var typeMapping = mapping.fldtype[fldtype];

      schemaType['type'] = typeMapping;
      schemaType.key = fieldDefinition.keyfld;
      schemaType.uppercase = fieldDefinition.uppercase;
      tableDef[lcFieldname] = schemaType;
    }
  }

  return tableDef;
}


function writeSchemas(schemas){
  var tables = [];
  for (table in schemas){
    if (!schemas.hasOwnProperty(table)){ continue; }

    var definitionFile = './db/schemas/'+ table +'.json';

    fs.writeFile(definitionFile,JSON.stringify(schemas[table]),function(err){
      if (err) throw err;
      console.log("saved ["+definitionFile+"]");
    });

    tables.push(table);
  }
  return tables;
}


function fixReturn( line ){
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

  return retrn.rows;
}

var stream = require('stream');
var util = require('util');
var mapping = require('./config/mapping.json');
var schemaTemplate = require('./config/template.json');
var fs = require('fs');
var schemas = {};

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

    tables = writeSchemas(schemaGen(mastdict));
    this.push(tables.toString(), 'utf8');

    cb();
};

function schemaGen(mastdict){
  for (table in mastdict){
      if (!mastdict.hasOwnProperty(table)){ continue; }
      var tableDefinition = mastdict[table];

      var lcTable = table.toLowerCase();
      var ucfTable = lcTable.charAt(0).toUpperCase() + lcTable.substr(1);

      schemas[lcTable] = schemaTemplate;
      schemas[lcTable].collection = ucfTable;

      if ( 'undefined' !== typeof (mapping.dataModel[lcTable]) ){
        var parent = mapping.dataModel[lcTable];
        var ucfParent = parent.charAt(0).toUpperCase() + parent.substr(1);
        schemas[lcTable].parent = ucfParent;
      }

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
          schemas[lcTable][lcFieldname] = schemaType;
        }
      }
    }
    return schemas;
}


function writeSchemas(schemas){
  var tables = [];
  for (table in schemas){
    if (!schemas.hasOwnProperty(table)){ continue; }
    
    var definitionFile = './schemas/'+ table +'.json';
    
    fs.writeFile(definitionFile,JSON.stringify(schemas[table]),function(err){
      if (err) throw err;
      console.log("saved ["+definitionFile+"]");
    });
    tables.push(table);  
  }
  return tables;
}

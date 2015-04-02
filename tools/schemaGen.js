var processTableDefinition = require('./processTableDefinition.js')
var mapping = require('../config/mapping.json');
var schemaTemplate = require('../config/template.json');

module.exports = function (mastdict){
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

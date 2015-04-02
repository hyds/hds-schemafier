var through = require('through2');
var tools = require('./tools');

module.exports = function(){
  var tables = [];
  return through(function write(buffer, _, next) {
    var string = buffer.toString().replace(/;$/g,"");
    console.log(string);
    try { var line = JSON.parse(string) }  
    catch (err){ return this.emit('error',err) }
    var mastdict = tools.fixReturn(line);
    tables = tools.writeSchemas(tools.schemaGen(mastdict));
    next();
  },
  function end(cb){
    this.push(tables.toString(), 'utf8');
    cb();
  })
}

var mongoose = require('mongoose'),
  hydstraSchema = mongoose.Schema({
    validkey : String,
    validcode : String,
    uppercase : Boolean,
    headlong : String,
    doco : String,
    dbname : String,
    fldname : String,
    fldtype : String,
    fldlen : Number,
    visible : Boolean,
    defvalue : String,
    allownul : Boolean,
    keyfld : Boolean,
    reserved : Boolean
  },{
  	collection : 'hydstra'
  });

module.exports = mongoose.model('hydstra', hydstraSchema);
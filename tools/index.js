module.exports = {
    schemaGen: require('./schemaGen'),
    writeSchemas: require('./writeSchemas'),
    processTableDefinition: require('./processTableDefinition'),
    fixReturn: require('./fixReturn') 
};



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

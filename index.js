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
  util.inherits(Hydstra, Transform);
  Transform.call(this, options);
}

function NRMGWDB(options) {
  // allow use without new
  if (!(this instanceof NRMGWDB)) {
    return new NRMGWDB(options);
  }

  // init Transform
  util.inherits(NRMGWDB, Transform);
  Transform.call(this, options);
}

NRMGWDB.prototype._transform = function (buf, enc, cb) {
}

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

		var fileText = 	"module.exports = mongoose.model('"+table+"', "+table+"Schema); var mongoose = require('mongoose'),
		"+table+"Schema = mongoose.Schema({"+JSON.stringify(schema)+"},{collection:'"+table+"'";

		fs.writeFile(file,fileText,function(err){
			if (err) throw err;
			console.log("saved ["+file+"]");
		});
 	}
    
    this.push(tables.toString()), 'utf8');

    cb();
};






/*

module.exports = { 
	geofy : function geofy(buf){
        var rs = new Readable;
        rs.push('beep ');
        rs.push('boop\n');
        rs.push(null);


        var input = through(write, end);
        var geojson = {
            "type": "FeatureCollection",
        };
        
        function write (row) {
        
            var line = JSON.parse(buf.toString().replace(/;$/,""));
            
            console.log("line [",line,"]")

            var sites = line._return.rows;
            var features = [];
            var feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                    }
            };

            for (var i = 0; i < sites.length; i++) {
                var coordinates = [];
                coordinates.push(sites[i].longitude);
                coordinates.push(sites[i].latitude);
                delete sites[i].longitude;
                delete sites[i].latitude;
                feature.geometry.coordinates = coordinates;
                feature.properties = sites[i];
                features.push(feature);
            }   

            geojson.features = features;

            

            

        
            
        }
        
        function end () { console.log('geojson [',geojson,']'); }

        return duplexer(input, geojson);

    }
}





function geofyOld (buf) {
    var line = JSON.parse(buf.toString().replace(/;$/,""));
    
    var sites = line._return.rows;
    var features = [];
    var feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
            }
    };

    for (var i = 0; i < sites.length; i++) {
        var coordinates = [];
        coordinates.push(sites[i].longitude);
        coordinates.push(sites[i].latitude);
        delete sites[i].longitude;
        delete sites[i].latitude;
        feature.geometry.coordinates = coordinates;
        feature.properties = sites[i];
        features.push(feature);
    }   

    geojson.features = features;

  	//console.log('geojson [',geojson,']');

	return geojson;
}

/*
{"station": "070048", "longitude": "149.44790000", "stname": "HOSKINTOWN RADIO
 OBSERVATORY (CBM)", "active": false, "stntype": "WEA", "elev": "1.910", "datemo
d": 20111111, "latitude": "-35.36840000", "lldatum": "GDA94", "orgcode": "CBM"}


*/
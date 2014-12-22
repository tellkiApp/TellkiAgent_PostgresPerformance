//node mysql_performance_monitor 192.168.69.3 1446 "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" 3306 "xpto" "xpto"

//java -jar postgresql-monitor-performance.jar 192.168.69.115 1449 "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" 5432 "postgres" "admin"

//node postgres_performance_monitor.js 192.168.69.115 1449 "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" 5432 "postgres" "admin"


var fs = require('fs');

var tempDir = "/tmp";

var metricsId =[];

metricsId["numbackends"] = {id:"483:4",ratio:false, type:"QUERY_STAT_DATABASE"};

metricsId["xact_commit"] = {id:"484:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["xact_rollback"] = {id:"485:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["blks_read"] = {id:"486:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["blks_hit"] = {id:"487:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["tup_returned"] = {id:"488:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["tup_fetched"] = {id:"489:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["tup_inserted"] = {id:"490:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["tup_updated"] = {id:"491:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["tup_deleted"] = {id:"492:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["deadlocks"] = {id:"493:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["temp_files"] = {id:"494:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["temp_bytes"] = {id:"495:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["blk_read_time"] = {id:"496:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metricsId["blk_write_time"] = {id:"497:4",ratio:true, type:"QUERY_STAT_DATABASE"};

metricsId["confl_tablespace"] = {id:"498:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metricsId["confl_lock"] = {id:"499:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metricsId["confl_snapshot"] = {id:"500:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metricsId["confl_bufferpin"] = {id:"501:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metricsId["confl_deadlock"] = {id:"502:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"}; 

metricsId["pg_database_size"] = {id:"503:4",ratio:false, type:"QUERY_STAT_DATABASE_SIZE"};

var metricsLength = 21;

var queries = []
queries["QUERY_STAT_DATABASE"] = {query:"SELECT * FROM pg_stat_database;", resultset:null};
queries["QUERY_STAT_DATABASE_CONFLICTS"] = {query:"SELECT * FROM pg_stat_database_conflicts;", resultset:null};
queries["QUERY_STAT_DATABASE_SIZE"] = {query:"SELECT pg_database_size(datid) FROM pg_stat_database;", resultset:null};

//####################### EXCEPTIONS ################################

function InvalidParametersNumberError() {
    this.name = "InvalidParametersNumberError";
    this.message = ("Wrong number of parameters.");
}
InvalidParametersNumberError.prototype = Error.prototype;

function InvalidMetricStateError() {
    this.name = "InvalidMetricStateError";
    this.message = ("Invalid number of metrics.");
}
InvalidMetricStateError.prototype = Error.prototype;

function InvalidAuthenticationError() {
    this.name = "InvalidAuthenticationError";
    this.message = ("Invalid authentication.");
}
InvalidAuthenticationError.prototype = Error.prototype;

// ############# INPUT ###################################

(function() {
	try
	{
		monitorInput(process.argv.slice(2));
	}
	catch(err)
	{	
		console.log(err.message);
		process.exit(1);
	}
}).call(this)



function monitorInput(args)
{
	if(args.length === 6)
	{
		monitorInputProcess(args);
	}
	else
	{
		throw new InvalidParametersNumberError()
	}
}


function monitorInputProcess(args)
{
	//host
	var hostname = args[0];
	
	//target
	var targetUUID = args[1];
	
	//metric state
	var metricState = args[2].replace("\"", "");
	
	var tokens = metricState.split(",");

	var metricsExecution = new Array(metricsLength);

	if (tokens.length === metricsLength)
	{
		for(var i in tokens)
		{
			metricsExecution[i] = (tokens[i] === "1")
		}
	}
	else
	{
		throw new InvalidMetricStateError();
	}
	
	//port
	var port = args[3];
	
	
	// Username
	var username = args[4];
	username = username.length === 0 ? "" : username;
	
	// Password
	var passwd = args[5];
	passwd = passwd.length === 0 ? "" : passwd;
	
	var requests = []
	
	var request = new Object()
	request.hostname = hostname;
	request.targetUUID = targetUUID;
	request.metricsExecution = metricsExecution;
	request.port = port;
	request.username = username;
	request.passwd = passwd;
	
	requests.push(request)

	//console.log(JSON.stringify(requests));
	
	monitorDatabasePerformance(requests);
	
}



function errorHandler(err)
{
	if(err)
	{
		console.log(err.message);
		process.exit(1);
	}
}


// ################# MONITOR ###########################
function monitorDatabasePerformance(requests) 
{
	var pg = require('pg');
	
	for(var i in requests)
	{
		var request = requests[i];
		
		var start = Date.now();
		
		var conString = "postgres://"+request.username+":"+request.passwd+"@"+request.hostname+":"+request.port+"/postgres";
		
		var client = new pg.Client(conString);
		
		client.connect(function(err) 
		{
			if (err && err.message.indexOf('authentication failed') > -1) 
			{
				errorHandler(new InvalidAuthenticationError());
			}
			else if(err)
			{
				errorHandler(err)
				return;
			}
			
			
			var run = 0;
			for(var i in queries)
			{
				getData(client, i, run, request);
				run ++;
			}
			
		});	
	}
}


function getData(client, index, run, request)
{
	var queryToExecute = queries[index].query;

	client.query(queryToExecute, function(err, result) {
		if(err) {
			console.log(err);
		}
		
		queries[index].resultset = result;
		
		if(run == 2)
		{
			client.end();
		
			extractData(request);
		}
			
	});	
}

function extractData(request)
{	
	var metricsName = Object.keys(metricsId);

	var jsonString = "[";

	var dateTime = new Date().toISOString();

	for(var i in metricsName)
	{
		if(request.metricsExecution[i])
		{	
			var results = queries[metricsId[metricsName[i]].type].resultset
			
			var total = 0;
			
			if(results != null)
			{	
				for(var j in results.rows)
				{
					total += parseFloat(results.rows[j][metricsName[i]]);
				}
			}
			
			jsonString += "{";
				
			jsonString += "\"variableName\":\""+metricsName[i]+"\",";
			jsonString += "\"metricUUID\":\""+metricsId[metricsName[i]].id+"\",";
			jsonString += "\"timestamp\":\""+ dateTime +"\",";
			jsonString += "\"value\":\""+ total +"\"";
			
			jsonString += "},";
			
		}
	}

	if(jsonString.length > 1)
		jsonString = jsonString.slice(0, jsonString.length-1);

	jsonString += "]";
	
	processDeltas(request, jsonString);

}



function processDeltas(request, results)
{
	var file = getFile(request.targetUUID);
	
	var toOutput = [];
	
	if(file)
	{		
		var previousData = JSON.parse(file);
		var newData = JSON.parse(results);
			
		for(var i = 0; i < newData.length; i++)
		{
			var endMetric = newData[i];
			var initMetric = null;
			
			for(var j = 0; j < previousData.length; j++)
			{
				if(previousData[j].metricUUID === newData[i].metricUUID)
				{
					initMetric = previousData[j];
					break;
				}
			}
			
			if (initMetric != null)
			{
				var deltaValue = getDelta(initMetric, endMetric, request);
				
				var rateMetric = new Object();
				rateMetric.id = endMetric.metricUUID;
				rateMetric.timestamp = endMetric.timestamp;
				rateMetric.value = deltaValue;
				
				toOutput.push(rateMetric);
			}
			else
			{	
				var rateMetric = new Object();
				rateMetric.id = endMetric.metricUUID;
				rateMetric.timestamp = endMetric.timestamp;
				rateMetric.value = 0;
				
				toOutput.push(rateMetric);
			}
		}
		
		setFile(request.targetUUID, results);

		for (var m = 0; m < toOutput.length; m++)
		{
			for (var z = 0; z < newData.length; z++)
			{
				var systemMetric = metricsId[newData[z].variableName];
				
				if (systemMetric.ratio === false && newData[z].metricUUID === toOutput[m].id)
				{
					toOutput[m].value = newData[z].value;
					break;
				}
			}
		}

		processOutput(request, toOutput)
		
	}
	else
	{
		setFile(request.targetUUID, results);
		process.exit(0);
	}
}



function processOutput(request, toOutput)
{
	var date = new Date().toISOString();

	for(var i in toOutput)
	{
		var output = "";
		var value = 0;
		
		if(toOutput[i].id === "495:4" || toOutput[i].id === "503:4")
		{
			// Convert bytes to MB.
			var valueToConvert = parseFloat(toOutput[i].value);	
			value = Math.floor((valueToConvert / 1024 / 1024) * 10) / 10;
		}
		else
		{
			value = toOutput[i].value;
		}
	
		output += date + "|";
		output += toOutput[i].id + "|";
		output += request.targetUUID + "|";
		output += value;
		
		console.log(output);
	}
	
	//process.exit(0);
}



function getDelta(initMetric, endMetric, request)
{
	var deltaValue = 0;

	var decimalPlaces = 2;
	//var deltaBD;

	var date = new Date().toISOString();
	
	if (parseFloat(endMetric.value) < parseFloat(initMetric.value))
	{	
		deltaValue = parseFloat(endMetric.value).toFixed(decimalPlaces);
	}
	else
	{	
		var elapsedTime = (new Date(endMetric.timestamp).getTime() - new Date(initMetric.timestamp).getTime()) / 1000;	
		deltaValue = ((parseFloat(endMetric.value) - parseFloat(initMetric.value))/elapsedTime).toFixed(decimalPlaces);
	}
	
	return deltaValue;
}



//########################################

function getFile(monitorId)
{
		var dirPath =  __dirname +  tempDir + "/";
		var filePath = dirPath + ".postgres_"+ monitorId+".dat";
		
		try
		{
			fs.readdirSync(dirPath);
			
			var file = fs.readFileSync(filePath, 'utf8');
			
			if (file.toString('utf8').trim())
			{
				return file.toString('utf8').trim();
			}
			else
			{
				return null;
			}
		}
		catch(e)
		{
			return null;
		}
}

function setFile(monitorId, json)
{
	var dirPath =  __dirname +  tempDir + "/";
	var filePath = dirPath + ".postgres_"+ monitorId+".dat";
		
	if (!fs.existsSync(dirPath)) 
	{
		try
		{
			fs.mkdirSync( __dirname+tempDir);
		}
		catch(e)
		{
			errorHandler(e);
		}
	}

	fs.writeFileSync(filePath, json);

}

var fs = require('fs');

var tempDir = "/tmp";

var metrics =[];

metrics["numbackends"] = {id:"483:Backends Connected:4",ratio:false, type:"QUERY_STAT_DATABASE"};
metrics["xact_commit"] = {id:"484:Transactions Committed:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["xact_rollback"] = {id:"485:Transactions Rolled Back:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["blks_read"] = {id:"486:Blocks Read:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["blks_hit"] = {id:"487:Blocks Hit:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["tup_returned"] = {id:"488:Rows Returned:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["tup_fetched"] = {id:"489:Rows Fetched:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["tup_inserted"] = {id:"490:Rows Inserted:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["tup_updated"] = {id:"491:Rows Updated:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["tup_deleted"] = {id:"492:Rows Deleted:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["deadlocks"] = {id:"493:Deadlocks:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["temp_files"] = {id:"494:Temporary Files:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["temp_bytes"] = {id:"495:Size of Temporary Files:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["blk_read_time"] = {id:"496:Bulk Read Time:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["blk_write_time"] = {id:"497:Bulk Write Time:4",ratio:true, type:"QUERY_STAT_DATABASE"};
metrics["confl_tablespace"] = {id:"498:Tablespace Conflicts:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metrics["confl_lock"] = {id:"499:Lock Conflicts:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metrics["confl_snapshot"] = {id:"500:Snapshot Conflicts:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metrics["confl_bufferpin"] = {id:"501:Bufferpin Conflicts:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"};
metrics["confl_deadlock"] = {id:"502:Deadlock Conflicts:4",ratio:true, type:"QUERY_STAT_DATABASE_CONFLICTS"}; 
metrics["pg_database_size"] = {id:"503:Disk Space Used:4",ratio:false, type:"QUERY_STAT_DATABASE_SIZE"};

var metricsLength = 21;

var queries = []
queries["QUERY_STAT_DATABASE"] = {query:"SELECT * FROM pg_stat_database;", resultset:null};
queries["QUERY_STAT_DATABASE_CONFLICTS"] = {query:"SELECT * FROM pg_stat_database_conflicts;", resultset:null};
queries["QUERY_STAT_DATABASE_SIZE"] = {query:"SELECT pg_database_size(datid) FROM pg_stat_database;", resultset:null};

//####################### EXCEPTIONS ################################

function InvalidParametersNumberError() {
    this.name = "InvalidParametersNumberError";
    this.message = "Wrong number of parameters.";
	this.code = 3;
}
InvalidParametersNumberError.prototype = Object.create(Error.prototype);
InvalidParametersNumberError.prototype.constructor = InvalidParametersNumberError;

function InvalidMetricStateError() {
    this.name = "InvalidMetricStateError";
    this.message = "Invalid number of metrics.";
	this.code = 9;
}
InvalidMetricStateError.prototype = Object.create(Error.prototype);
InvalidMetricStateError.prototype.constructor = InvalidMetricStateError;

function InvalidAuthenticationError() {
    this.name = "InvalidAuthenticationError";
    this.message = "Invalid authentication.";
	this.code = 2;
}
InvalidAuthenticationError.prototype = Object.create(Error.prototype);
InvalidAuthenticationError.prototype.constructor = InvalidAuthenticationError;

function DatabaseConnectionError(message) {
	this.name = "DatabaseConnectionError";
    this.message = message;
	this.code = 11;
}
DatabaseConnectionError.prototype = Object.create(Error.prototype);
DatabaseConnectionError.prototype.constructor = DatabaseConnectionError;

function CreateTmpDirError(message)
{
	this.name = "CreateTmpDirError";
    this.message = message;
	this.code = 21;
}
CreateTmpDirError.prototype = Object.create(Error.prototype);
CreateTmpDirError.prototype.constructor = CreateTmpDirError;


function WriteOnTmpFileError(message)
{
	this.name = "WriteOnTmpFileError";
    this.message = message;
	this.code = 22;
}
WriteOnTmpFileError.prototype = Object.create(Error.prototype);
WriteOnTmpFileError.prototype.constructor = WriteOnTmpFileError;

// ############# INPUT ###################################

(function() {
	try
	{
		monitorInput(process.argv.slice(2));
	}
	catch(err)
	{	
		if(err instanceof InvalidParametersNumberError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof InvalidMetricStateError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof InvalidAuthenticationError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else if(err instanceof DatabaseConnectionError)
		{
			console.log(err.message);
			process.exit(err.code);
		}
		else
		{
			console.log(err.message);
			process.exit(1);
		}
	}
}).call(this)



function monitorInput(args)
{
	if(args.length === 5)
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
	
	//metric state
	var metricState = args[1].replace("\"", "");
	
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
	var port = args[2];
	
	
	// Username
	var username = args[3];
	
	username = username.length === 0 ? "" : username;
	username = username === "\"\"" ? "" : username;
	if(username.length === 1 && username === "\"")
		username = "";
	
	// Password
	var passwd = args[4];
	
	passwd = passwd.length === 0 ? "" : passwd;
	passwd = passwd === "\"\"" ? "" : passwd;
	if(passwd.length === 1 && passwd === "\"")
		passwd = "";
	
	var requests = []
	
	var request = new Object()
	request.hostname = hostname;
	request.metricsExecution = metricsExecution;
	request.port = port;
	request.username = username;
	request.passwd = passwd;
	
	requests.push(request)
	
	monitorDatabasePerformance(requests);
	
}

// ############# OUTPUT ###################################

function output(toOutput)
{
	for(var i in toOutput)
	{
		var out = "";
		var value = 0;
		
		if(toOutput[i].id === "495:Size of Temporary Files:4" || toOutput[i].id === "503:Disk Space Used:4")
		{
			// Convert bytes to MB.
			var valueToConvert = parseFloat(toOutput[i].value);	
			value = Math.floor((valueToConvert / 1024 / 1024) * 10) / 10;
		}
		else
		{
			value = toOutput[i].value;
		}
	
		out += toOutput[i].id + "|";
		out += value + "|";
		
		console.log(out);
	}
}



function errorHandler(err)
{
	if(err instanceof InvalidAuthenticationError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof DatabaseConnectionError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof CreateTmpDirError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else if(err instanceof WriteOnTmpFileError)
	{
		console.log(err.message);
		process.exit(err.code);
	}
	else
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
				errorHandler(new DatabaseConnectionError(err.message));
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
			errorHandler(new DatabaseConnectionError(err.message));
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
	var metricsName = Object.keys(metrics);

	var jsonString = "[";

	var dateTime = new Date().toISOString();

	for(var i in metricsName)
	{
		if(request.metricsExecution[i])
		{	
			var results = queries[metrics[metricsName[i]].type].resultset
			
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
			jsonString += "\"metricUUID\":\""+metrics[metricsName[i]].id+"\",";
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
	var file = getFile(request.hostname, request.port);
	
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
				var deltaValue = getDelta(initMetric, endMetric);
				
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
		
		setFile(request.hostname, request.port, results);

		for (var m = 0; m < toOutput.length; m++)
		{
			for (var z = 0; z < newData.length; z++)
			{
				var systemMetric = metrics[newData[z].variableName];
				
				if (systemMetric.ratio === false && newData[z].metricUUID === toOutput[m].id)
				{
					toOutput[m].value = newData[z].value;
					break;
				}
			}
		}

		output(toOutput)
		
	}
	else
	{
		setFile(request.hostname, request.port, results);
		process.exit(0);
	}
}




function getDelta(initMetric, endMetric)
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

function getFile(hostname, port)
{
		var dirPath =  __dirname +  tempDir + "/";
		var filePath = dirPath + ".postgres_"+ hostname +"_"+ port +".dat";
		
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

function setFile(hostname, port, json)
{
	var dirPath =  __dirname +  tempDir + "/";
	var filePath = dirPath + ".postgres_"+ hostname +"_"+ port +".dat";
		
	if (!fs.existsSync(dirPath)) 
	{
		try
		{
			fs.mkdirSync( __dirname+tempDir);
		}
		catch(e)
		{
			errorHandler(new CreateTmpDirError(e.message));
		}
	}

	try
	{
		fs.writeFileSync(filePath, json);
	}
	catch(err)
	{
		errorHandler(new WriteOnTmpFileError(err.message));
	}

}

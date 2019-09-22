# gcp_log_toolbox.py    
gcp_log_toolbox.py is a python 3.5+ tool created to assist in the collection and manipulatation of Google Cloud Platform (GCP) logs in json format. It currently supports the following functions:  

`Collection` - Download StackDriver log exports from Google Cloud Storage  
`Analysis` - Calculate basic statistics about json log file (for the purpose of understanding what to filter in/out of a log)  
`Manipulation` - Combine multiple json log files  
`Manipulation` - Remove certain fields from a json log file  
`Manipulation` - Filter a json log for a set of specified fields  
`Manipulation` - Create a timeslice (+-x minutes) from a json log file  
`Manipulation` - Create a timeframe (datetime > datetime) from a json log file

The output of gcp_log_toolbox.py functions are compatible with gcp_timeliner.py, a tool to process a gcp json log file for colourised analysis in Excel.

## Format support
gcp_log_toolbox.py is designed to work with a json lines. It iterates through the lines to avoid loading the full file into memory. E.g.
```
{"insertId":"6y6dfde2p4wu","logName":"projects [...] }
{"insertId":"6y6dfde2p4ww","logName":"projects [...] }
{"insertId":"-ajhlaze80biz","logName":"projects [...] }
```

The `--gcloudformat` function of gcp_log_toolbox.py can convert arrays of json logs (as produced by 'gcloud logging read') into single line json format to support other functions of this tool.

## Dependancies
google_cloud_toolbox.py utilises pandas for data analysis, and google-cloud-storage for accessing GCP. You can install these modules using the following command:
```
pip install -r requirements.txt
```

## Collection

### Cloud Storage - Log Exports

gcp_log_toolbox.py can download logs which have been exported to google cloud storage. It is recommended to use a service account with 'Viewer' permissions to the cloud storage bucket containg the logs. 

Permissions can be inherited from gcloud configuration (default) or a .json key file can be passed to gcp_log_toolbox.py using the --key argument

Syntax:
```
python gcp_log_toolbox.py --download cloudstorage --bucketid <bucket id> --key serviceaccount.json --output .\local\output\folder
```
If you would like to collect specific logs, you can include a wildcard filter using the '-f' argument.  

Syntax:
```
python gcp_log_toolbox.py --download cloudstorage --bucketid <bucket id> --key serviceaccount.json -f *2019* -o .\local\output\folder
```

### StackDriver - Log Viewer
Limitations in the GCP python library make it difficult to download json in a compatible format.
Until I figure out how to resolve this, please follow this process:

Step 1 - Download an array of logs with the gcloud tool
```
gcloud logging read '<advanced_filter>' --freshness=7d --order=asc --format='json' > output.json  
```
note: you can leave the filter blank '' for all logs


Step 2 - Convert the output to gcp_log_toolbox compatible format with the following command:
```
python gcp_log_toolbox.py --gcloudformatter -f .\input.json -o .\output.json
```

This may cause errors for very big logs as gcp_log_toolbox.py currently has to read the whole file into memory before splitting it up. I'll try
resolve this later.


## Analysis
gcp_log_toolbox.py can produce the following statistics about a given json log.
* Number of logs
* Log chronology (first and last)
* Number of logs by resource type
* Number of logs by severity

This can assist in deciding how to filter your log file for timeline processing with gcp_timeliner.py

 Synatx:  
```
python .\gcp_log_toolbox.py --statistics -f .\log.json
```

## Manipulation

### Merge multiple json log files  
gcp_log_toolbox.py can merge multiple log files into one log file.

Syntax:
```
python .\gcp_log_toolbox.py --merge -f .\exports\*.json --recurse -o .\output.json
```

### Extract fields from a json log file 
gcp_log_toolbox.py can extract fields from a log file into a new log file based on json fields. Multiple fields can be provided using comma separation.

Syntax:
```
python .\gcp_log_toolbox.py --filter include -t "resource.type=k8s_cluster,severity=WARNING" -f .\input.json -o .\output.json
```

### Exclude fields from a json log file 
gcp_log_toolbox.py can create a new log file, excluding specified fields from an existing log file. Multiple fields can be provided using comma separation.

Syntax:
```
python .\gcp_log_toolbox.py --filter exclude -t "severity=INFO" -f .\input.json -o .\output.json
```

### Create a timeslice (+-x minutes)  
gcp_log_toolbox.py can create a new log based on a specified timeslice. The default timeslice time is 5 minutes, however a custom slice time (minutes) can be set with the -s/--size argument

Syntax:
```
python .\gcp_log_toolbox.py --timeslice "2019-07-23 12:00:00" -s 60 -f .\input.json -o .\output.json
```

### Create a timeframe (datetime > datetime)  
gcp_log_toolbox.py can create new log based on a timeframe. 

Syntax:
```
python .\gcp_log_toolbox.py --timeframe "2019-07-23 00:00:00 > 2019-07-23 13:23:06" -f .\input.json -o .\output.json
```


import os
import sys
import glob
import json
import fnmatch
import logging
import pathlib
import argparse
import pandas as pd
from datetime import datetime
from datetime import timedelta
from google.cloud import storage
from google.cloud.exceptions import NotFound


def readLog(logPath):
    """Reads a file containing an array of json objects. \
        Used with the 'gcloudformatter' function.

    Args:
        logPath: file system path to map file

    Returns:
        An array of json objects
    """

    logger.info("Reading log {}".format(logPath))
    with open(logPath, 'r') as f:
        log = json.load(f)
    return log


def validateArgs(args):
    """Validates argument conditions.

    Args:
        args: arguments

    Returns:
        Nothing
    """
    if args.statistics is True:
        if args.file is None:
            parser.error("--statistics requires -f/--file")
    if args.timeslice is not None:
        if args.file is None:
            parser.error("--timeslice requires -f/--file")
        if args.output is None:
            parser.error("--timeslice requires -o/--output")
    if args.timeframe is not None:
        if args.file is None:
            parser.error("--timeframe requires -f/--file")
        if args.output is None:
            parser.error("--timeframe requires -o/--output")
    if args.merge is True:
        if args.file is None:
            parser.error("--merge requires -f/--file path. \
                        Wildcards and --recurse accepted")
        if args.output is None:
            parser.error("--merge requires -o/--output")
    if args.filter is not None:
        if args.file is None:
            parser.error("--filter requires -f/--file")
        if args.type is None:
            parser.error("--filter requires -t/--type")
        if args.output is None:
            parser.error("--filter requires -o/--output")
    if args.download == 'cloudstorage':
        if args.key is None:
            print("No service account key identified. \
                Using default configuration.")
        if args.file is not None:
            print("Using path filter: {}".format(args.file))
        if args.bucketid is None:
            parser.error("BucketId is required. E.g. -b myproject-logarchive")
        if args.output is None:
            parser.error("Output folder required. \
                        E.g. -o .\\download\\cloudstorage")
    if args.gcloudformatter is True:
        if args.file is None:
            parser.error("--gcloudformatter requires -f/--file")
        if args.file is None:
            parser.error("--gcloudformatter requires -o/--output")


def continuePrompt(cont):
    """Prompt to confirm the user wants to continue.

    Args:
        cont: True/False to bypass user confirmation to continue

    Returns:
        None
    """
    if cont is not True:
        answer = None
        while answer not in (
                            "yes",
                            "Yes",
                            "Y",
                            "y",
                            "YES",
                            "no",
                            "No",
                            "N",
                            "n",
                            "NO"):
            answer = input("Continue? (y/n): ")
            if (answer == "yes" or
                    answer == "Yes" or
                    answer == "Y" or
                    answer == "y" or
                    answer == "YES"):
                pass
            elif (answer == "no" or
                    answer == "No" or
                    answer == "N" or
                    answer == "n" or
                    answer == "No"):
                sys.exit()
            else:
                print("Please enter yes or no.")
    return


def writeOutput(data, encode, output):
    """Writes data to file

    Args:
        data: The data to write.
        encode: True/False to determine whether to encode string as json string or not.
        output: output file

    Returns:
        None
    """
    logger.debug("writing output to: {}".format(output))
    try:
        with open(output, 'a+') as o:
            if encode is True:
                json.dump(data, o)
                o.write("\n")
                o.close
            else:
                o.write(data)
                o.close
        return
    except (OSError, ValueError):
        if 'insertId' in data:
            raise Exception(logger.warning("Error: Failed to write log {}".format(data['insertId'])))
        else:
            raise Exception(logger.warning("Error: Failed to write output"))
        return


def pdFrame(file):
    """Creates a pandas data frame from a json log file

    Args:
        file: json log file to read

    Returns:
        pandas data frame
    """
    logger.debug("creating pandas data frame from {}".format(file))
    data = []
    with open(file) as f:
        for line in f:
            tmp = []
            log = json.loads(line)
            try:
                tmp.append(pd.Timestamp(log['timestamp']))
            except KeyError:
                tmp.append('no value')
            try:
                tmp.append(str(log['resource']['type']))
            except KeyError:
                tmp.append('no value')
            try:
                tmp.append(str(log['severity']))
            except KeyError:
                tmp.append('no value')
            try:
                tmp.append(str(log['protoPayload']['authenticationInfo']['principalEmail']))
            except KeyError:
                tmp.append('no value')
            data.append(tmp)

    fieldNames = ['timestamp', 'resourceType', 'severity', 'account']
    logs = pd.DataFrame(data, columns=fieldNames)
    return logs


def statistics_len(logs):
    """Calculates number of entries in a pandas data frame

    Args:
        logs: Pandas data frame

    Returns:
        x: length of data frame index
    """
    logger.debug("calculating statistics_len")
    x = len(logs.index)
    return x


def statistics_chronology(logs):
    """Calculates earliest and latest timestamp in a pandas data frame

    Args:
        logs: Pandas data frame

    Returns:
        minVal: earliest date
        maxVal: latest date
    """
    logger.debug("calculating statistics_chronology")
    minVal = logs['timestamp'].min()
    maxVal = logs['timestamp'].max()
    return minVal, maxVal


def statistics_byType(logs):
    """Calculates a unique count of items per resourceType in a pandas data frame

    Args:
        logs: Pandas data frame

    Returns:
        x: count of items by resourceType
    """
    logger.debug("calculating statistics by type")
    x = logs["resourceType"].value_counts()
    return x


def statistics_byAccount(logs):
    """Calculates a unique count of items per account in a pandas data frame

    Args:
        logs: Pandas data frame

    Returns:
        x: count of items by account
    """
    logger.debug("calculating statistics by account")
    x = logs["account"].value_counts()
    return x


def statistics_bySeverity(logs):
    """Calculates a unique count of items per severity in a pandas data frame

    Args:
        logs: Pandas data frame

    Returns:
        x: count of items by severity
    """
    logger.debug("calculating statistics by severity")

    x = logs["severity"].value_counts()
    return x


def statistics(file):
    """Displays statistics about the contents of a GCP json log.

    Args:
        file: The file to analyse

    Returns:
        None
    """
    logs = pdFrame(file)

    # Total Logs
    print("---------------------")
    print("Total log count")
    print("---------------------")
    print(statistics_len(logs))
    print("\n")

    print("---------------------")
    print("Chronology")
    print("---------------------")
    minVal, maxVal = statistics_chronology(logs)
    print("Oldest Log: {}".format(minVal))
    print("Most Recent Log: {}".format(maxVal))
    print("\n")

    # Logs by type
    print("---------------------")
    print("Logs by resource.type")
    print("---------------------")
    print(statistics_byType(logs))
    print("\n")

    # Logs by account
    print("---------------------")
    print("Logs by account")
    print("---------------------")
    print(statistics_byAccount(logs))
    print("\n")

    # Logs by severity
    print("---------------------")
    print("Logs by severity")
    print("---------------------")
    print(statistics_bySeverity(logs))
    print("\n")


def convertTimeString(s):
    """Converts a string to a datetime object

    Args:
        s: string

    Returns:
        dateTimeVal: datetime object
    """
    logger.debug("Converting time string {}".format(s))

    try:
        dateTimeVal = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        return dateTimeVal
    except ValueError:
        print(logger.warning("Failed to parse date/time: {}".format(s)))
        sys.exit()


def getTimeDeltas(dateTimeVal, size):
    """Generates a start time an end time slice from a given datetime object

    Args:
        dateTimeVal: datetime object
        size: time slice size in minutes

    Returns:
        startDateTime: starting datetime object of time slice
        endDateTime: ending datetime object of time slice
    """
    try:
        startDateTime = dateTimeVal - timedelta(minutes=(size/2))
        endDateTime = dateTimeVal + timedelta(minutes=(size/2))
        return startDateTime, endDateTime
    except ValueError:
        print(logger.warning("Failed to build time deltas from: {}".format(dateTimeVal)))
        sys.exit()


def timeslice(file, cont, output, size, dateTimeString):
    """Creates a new log file containing logs x seconds plus or minus a given timestamp.

    Args:
        file: input file
        cont: True/False to accept continue prompts automatically
        output: output file
        size: timeline size in minutes
        dateTimeString: datetime string to create timeslice from

    Returns:
        None
"""
    dateTimeVal = convertTimeString(dateTimeString)
    startDateTime, endDateTime = getTimeDeltas(dateTimeVal, size)
    logger.info("Start Date/Time: {}".format(startDateTime))
    logger.info("End Date/Time: {}".format(endDateTime))

    continuePrompt(cont)

    with open(file) as f:
        for line in f:
            logger.debug("reading {} line by line".format(file))
            log = json.loads(line)
            tmp = datetime.strptime(log['timestamp'][0:19],
                                    '%Y-%m-%dT%H:%M:%S')
            if tmp >= startDateTime and tmp <= endDateTime:
                writeOutput(log, True, output)


def timeframe(file, cont, output, timeframe):
    """Creates a new log file containing logs between two given datetime values.

    Args:
        file: input file
        cont: True/False to accept continue prompts automatically
        output: output file
        timeframe: datetime > datetime

    Returns:
        None
    """
    try:
        tmp = timeframe.split(">")
        startDateTime = convertTimeString(tmp[0].strip())
        endDateTime = convertTimeString(tmp[1].strip())
    except ValueError:
        logger.warning("Failed to parse date/time. \
            Format should be YYYY-MM-DD HH:MM:SS - YYYY-MM-DD HH:MM:SS")
        sys.exit()

    logger.info("Start Date/Time: {}".format(startDateTime))
    logger.info("End Date/Time: {}".format(endDateTime))
    continuePrompt(cont)

    with open(file) as f:
        for line in f:
            log = json.loads(line)
            tmp = datetime.strptime(log['timestamp'][0:19], '%Y-%m-%dT%H:%M:%S')
            if tmp >= startDateTime and tmp <= endDateTime:
                writeOutput(log, True, output)


def getFileListing(files, recurse):
    """Creates recursive or non-recursive file listing.

    Args:
        file: path to evaluate
        recurse: True/False value which dictates whether the listing is recursive

    Returns:
        fileList: array containing the results of the file listing
    """
    logger.debug("getting file listing. Recurse == {}".format(recurse))
    path = pathlib.Path(files).expanduser()
    parts = path.parts
    rootDirTmp = parts[:-1]
    rootDir = pathlib.Path(*rootDirTmp)
    searchTmp = parts[len(parts)-1:]
    search = pathlib.Path(*searchTmp)
    fileList = []
    if recurse is True:
        for filename in glob.iglob(str(rootDir) + "/**/" + str(search), recursive=recurse):
            if os.path.isfile(filename):
                fileList.append(filename)
    else:
        for filename in glob.iglob(str(rootDir) + "/" + str(search), recursive=recurse):
            if os.path.isfile(filename):
                fileList.append(filename)
    return fileList


def mergeLogs(files, cont, output, recurse):
    """Merges multiple logs from a directory (recursion supported) into one log.

    Args:
        file: path to evaluate
        cont: True/False to accept continue prompts automatically
        output: output file path
        recurse: True/False value which dictates whether the listing is recursive

    Returns:
        None
    """
    fileList = getFileListing(files, recurse)
    if len(fileList) == 0:
        raise Exception(logger.warning("No files identified. Did you mean to --recurse?"))
    for a in fileList:
        print(a)

    continuePrompt(cont)
    logger.info("Merging files...")
    for item in fileList:
        try:
            with open(item, 'r') as i:
                writeOutput(i.read(), False, output)
        except OSError:
            raise Exception(logger.warning("Error: Failed to open {}".format(item)))
    return


def getBlobs(client, bucketId, file, cont):
    """Obtains a list of blobs in a google cloud storage directory
    Args:
        clilent: GCP client object
        bucketId: GCP bucket ID
        file: path filter. E.g. *2019*
        cont: True/False to accept continue prompts automatically

    Returns:
        blobList: list of blob paths
    """
    logger.debug("Downloading from {} with filter: {} (acceptall == {}".format(bucketId, file, cont))
    blobs = client.list_blobs(bucketId)
    blobList = []
    totalSize = 0
    logger.info("-------------------")
    logger.info("Identified objects")
    logger.info("-------------------")

    if args.file is not None:
        for blob in blobs:
            if fnmatch.fnmatch(blob.name, file):
                totalSize += blob.size
                blobList.append(blob.name)
                logger.info(blob.name)
    else:
        for blob in blobs:
            totalSize += blob.size
            blobList.append(blob.name)
            logger.info(blob.name)
    kb = float(totalSize/1024)
    mb = round(kb/1024, 2)
    logger.info('Download size: {} bytes (approx. {} mb)'.format(totalSize, mb))
    continuePrompt(cont)

    return blobList


def blobDownload(blobItem, bucketId, output):
    """Downloads blob item from GCP cloud storage
    Args:
        client: GCP client object
        bucketId: GCP bucket ID
        output: output directory

    Returns:
        None
    """
    logger.debug("Downloading {} from {} to {}".format(blobItem, bucketId, output))

    try:
        client = storage.Client()
        bucket = client.get_bucket(bucketId)
        blob = bucket.blob(blobItem)
        blobPath = pathlib.Path(blobItem)
        tmp = str(blobPath).replace(":", "-")
        blobPathNew = pathlib.Path(tmp)
        localFolder = pathlib.Path.cwd() / output / blobPathNew.parent
        localFullPath = pathlib.Path.cwd() / output / blobPathNew
        os.makedirs(localFolder, exist_ok=True)
        blob.download_to_filename(localFullPath)
        logger.info('{} downloaded to {}.'.format(blobPath.name, localFullPath))
    except NotFound:
        logger.warning("Failed to download {}".format(blobPath.name))

    return


def downloadCloudStorage(bucketId, cont, file, output):
    """Establishes connection to GCP and calls blob listing and download functions
    Args:
        bucketId: ID of GCP cloud storage bucket
        cont: True/False to accept continue prompts automatically
        file: Path filter e.g. *2019*
        output: Output directory

    Returns:
        None
    """

    client = storage.Client()
    blobList = getBlobs(client, bucketId, file, cont)
    if len(blobList) == 0:
        raise Exception(logger.warning("No blob objects identified"))
    elif len(blobList) > 0:
        for blob in blobList:
            blobDownload(blob, bucketId, output)
    else:
        raise Exception(logger.warning("Invalid number of blobs identified (not 0, 1, or > 1"))


def parseFilters(filterString, filter):
    """Parses user provided filter string into array conditions for use in filterLog
    Args:
        filterString: include or exclude
        filter: user provided filter string

    Returns:
        None
    """
    logger.debug("parsing filter to {} parameters: {}".format(filterString, filter))
    filterList = []
    logger.info('{} logs that match the following conditions?'.format(filterString))
    for f in (filter.split(",")):
        logger.info('[*] {}'.format(f))
        x = (f.split("="))
        filterList.append(x)
    return filterList


def filterLog(file, cont, output, filterVal, filterString):
    """Filters json logs based on user provided filter parameters
    Args:
        file: path to json log file
        cont: True/False to accept continue prompts automatically
        output: output file path
        filterVal: include or exclude
        filterString: User provided string containing filter parameters (comma separated)

    Returns:
        None
    """
    filterList = parseFilters(filterString, filterVal)

    continuePrompt(cont)

    with open(file) as f:
        for line in f:
            log = json.loads(line)
            excludeCount = 0
            for item in filterList:
                fields = item[0].split(".")
                value = item[1].strip()
                if filterString == "include":
                    try:
                        if len(fields) == 1:
                            if log[fields[0]] == value:
                                writeOutput(log, True, output)
                        elif len(fields) == 2:
                            if log[fields[0]][fields[1]] == value:
                                writeOutput(log, True, output)
                        elif len(fields) == 3:
                            if log[fields[0]][fields[1]][fields[2]] == value:
                                writeOutput(log, True, output)
                        elif len(fields) == 4:
                            if log[fields[0]][fields[1][fields[2]][fields[3]]] == value:
                                writeOutput(log, True, output)
                        elif len(fields) == 2:
                            if log[fields[0]][fields[1]][fields[2]][fields[3]][fields[4]] == value:
                                writeOutput(log, True, output)
                        elif len(fields) == 2:
                            if log[fields[0]][fields[1]][fields[2]][fields[3]][fields[4]][fields[5]] == value:
                                writeOutput(log, True, output)
                    except KeyError:
                        logger.debug("Key Error at log {}".format(log['insertId']))
                        pass
                if filterString == "exclude":
                    try:
                        if len(fields) == 1:
                            if log[fields[0]] != value:
                                excludeCount += 1
                        elif len(fields) == 2:
                            if log[fields[0]][fields[1]] != value:
                                excludeCount += 1
                        elif len(fields) == 3:
                            if log[fields[0]][fields[1]][fields[2]] != value:
                                excludeCount += 1
                        elif len(fields) == 4:
                            if log[fields[0]][fields[1][fields[2]][fields[3]]] != value:
                                excludeCount += 1
                        elif len(fields) == 2:
                            if log[fields[0]][fields[1]][fields[2]][fields[3]][fields[4]] != value:
                                excludeCount += 1
                        elif len(fields) == 2:
                            if log[fields[0]][fields[1]][fields[2]][fields[3]][fields[4]][fields[5]] != value:
                                excludeCount += 1
                    except KeyError:
                        excludeCount += 1
            if filterString == "exclude" and excludeCount == len(filterList):
                writeOutput(log, True, output)


def gcloudFormatter(file, output):
    """Converts an array of json log (like that produced by 'gcloud logging read') to single line json format)
    Args:
        file: path to json log file
        output: output file path

    Returns:
        None
    """
    logger.debug("reformatting gloud array {} to single line json file {}".format(file, output))

    with open(file) as f:
        log = ''
        count = 0
        notify = 10000
        for line in f:
            logger.debug("Processed logs {}".format(count))
            if count == notify:
                logger.info("Processed logs: {}".format(count))
                notify += 10000
            if (line.strip()).startswith('['):
                pass
            elif line.startswith("  {"):
                log += line.strip()
            elif line.startswith("  }") and (line.strip()).endswith(","):
                line = line[:-2]
                log += line.strip()
                writeOutput(log, False, output)
                log = '\n'
                count += 1
            elif line.startswith("  }"):
                line = line[:-1]
                log += line.strip()
                writeOutput(log, False, output)
                log = '\n'
                count += 1
            elif line.startswith(']'):
                logger.info("Finished formatting {} to {}".format(file, output))
            else:
                if len(line) > 5:
                    log += line.strip()
    return


def downloadStackdriver():
    print("-------------------------------------------------------------------\
            --------------------------------------")
    print("Limitations in the GCP python library make it difficult to download\
            json in a compatible format. \nUntil I figure out how to resolve this\
            , please follow this process:\n")
    print("Step 1 - Download an array of logs with the gcloud tool: \n \
            gcloud logging read '<advanced_filter>' --freshness=7d --order=asc \
            --format='json' > output.json")
    print("note: you can leave the filter blank '' for all logs")
    print("Step 2 - Convert the output to gcp_log_toolbox compatible \
            format with the following command: \npython gcp_log_toolbox.py \
            --gcloudformatter -f .\\input.json -o .\\output.json")
    print("This may cause errors for very big logs as it has to read the whole\
            file before splitting it up. I'll try\nresolve this later if I don't \
            find a better approach")
    print("-------------------------------------------------------------------\
            --------------------------------------")

logger = logging.getLogger(__name__) # 'root' Logger

if __name__ == "__main__":
    # Argument setup
    parser = argparse.ArgumentParser()
    task = parser.add_mutually_exclusive_group(required=True)
    task.add_argument("--download", help='Download json logs from GCP cloud \
        storage or stackdriver (soon). Usage: gcp_log_toolbox.py --download \
            cloudstorage -b companylog-archive \
                -o ./outputdir', choices=['stackdriver', 'cloudstorage'])
    task.add_argument("--merge",  help="Merges two or more json log files into\
        one json log file. Usage: gcp_log_toolbox.py --merge -f ./logdir/*.json\
            -o ./output.json --recurse", action="store_true")
    task.add_argument("--statistics", help="Displays statistics about a single\
        json log file. Usage: gcp_log_toolbox.py --statistics -f \
            ./log.json", action="store_true")
    task.add_argument("--timeslice", help='Create a time slice around a specific\
        datetime. Usage: gcp_log_toolbox.py --timeslice "yyyy-mm-dd hh:mm:ss" \
            -f ./log.json -s 60 -o ./output.json')
    task.add_argument("--timeframe", help='Filter logs between two specified \
        dates. Usage: gcp_log_toolbox.py --timeframe "yyyy-mm-dd hh:mm:ss - \
            yyyy-mm-dd hh:mm:ss" -f ./log.json -o ./output.json')
    task.add_argument("--filter", help='Filter existing log file to exclude or \
        include logs of a specified resource.type', choices=['include', 'exclude'])
    task.add_argument("--gcloudformatter", help='Convert gcloud logging read \
        output (array of json) to gcp_log_toolbox format (single line compressed\
            json)', action="store_true", default=False)
    parser.add_argument("-b", "--bucketid", help="Bucket ID of GCP bucket to \
        download logs from")
    parser.add_argument("-f", "--file", help="GCP log file(s) for processing \
        (json format). Supports wildcard for merge and download (cloudstorage) \
            functions only.")
    parser.add_argument("-o", "--output", help="Output file or output directory \
        (depending on function). Used for statistics (single file), timeslice \
            (single file), download (folder) and merge (wildcard supported).")
    parser.add_argument("-r", "--recurse", help="Option to recurse through \
        directory to identify GCP log files. Used for merge \
            only.", action="store_true", default=False)
    parser.add_argument("-s", "--size", help="Time slice size in \
        minutes", type=int, default=30)
    parser.add_argument("-t", "--type", help="Json values to include or exclude\
        from log. Supports comma separation for multiple values. For use with \
            'filter' function.")
    parser.add_argument("-k", "--key", help="path to json key file \
        (for authentication).")
    parser.add_argument("--acceptall", help="Accept all prompts without \
        user input", action="store_true", default=False)
    parser.add_argument("-v", "--verbose", help="Verbose logs \
        ", action="store_true", default=False)
    args = parser.parse_args()

    validateArgs(args)

    console = logging.StreamHandler()
    logger.addHandler(console) # prints to console.
    if args.verbose is True:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if args.statistics is True:
        statistics(args.file)

    if args.timeslice is not None:
        timeslice(args.file, args.acceptall, args.output, args.size, args.timeslice)

    if args.timeframe is not None:
        timeframe(args.file, args.acceptall, args.output, args.timeframe)

    if args.merge is True:
        mergeLogs(args.file, args.acceptall, args.output, args.recurse)

    if args.download == 'cloudstorage':
        downloadCloudStorage(args.bucketid, args.acceptall, args.file, args.output)

    if args.download == 'stackdriver':
        downloadStackdriver()

    if args.filter is not None:
        filterLog(args.file, args.acceptall, args.output, args.type, args.filter)

    if args.gcloudformatter is True:
        gcloudFormatter(args.file, args.output)

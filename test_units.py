import os
import gcp_log_toolbox
from datetime import datetime

# To use this file, run the following command from this directory:
# pytest test_functions.py -v


def test_readLog():
    testVal = gcp_log_toolbox.readLog("./unit_test_logs/gcloud_array_small.json")
    assert len(testVal) == 555


def test_continuePrompt_True():
    testVal = gcp_log_toolbox.continuePrompt(True)
    assert testVal is None


def test_write_Output_No_Encoding():
    with open("./unit_test_logs/cloud_storage_sink/cloudaudit.googleapis.com/activity/2019/06/16/08-00-00_08-59-59_S0.json") as f:
        tmpA = f.read()
        a = len(tmpA)

    with open("./unit_test_logs/cloud_storage_sink/cloudaudit.googleapis.com/activity/2019/06/16/08-00-00_08-59-59_S0.json") as f:
        for line in f:
            gcp_log_toolbox.writeOutput(line, False, "./unit_test_logs/tmp.json")

    with open("./unit_test_logs/tmp.json") as n:
        tmpB = n.read()
        b = len(tmpB)

    assert a == b
    os.remove("./unit_test_logs/tmp.json")


def test_statistics_len():
    data = gcp_log_toolbox.pdFrame("./unit_test_logs/json_lines_small.json")
    tmpVal = gcp_log_toolbox.statistics_len(data)
    assert tmpVal == 555


def test_statistics_chronology():
    data = gcp_log_toolbox.pdFrame("./unit_test_logs/json_lines_small.json")
    minVal, maxVal = gcp_log_toolbox.statistics_chronology(data)
    assert str(minVal) == "2019-07-22 20:04:31.212077+00:00"
    assert str(maxVal) == "2019-07-23 13:23:06.608000+00:00"


def test_statistics_byType():
    data = gcp_log_toolbox.pdFrame("./unit_test_logs/json_lines_small.json")
    tmpVal = gcp_log_toolbox.statistics_byType(data)
    assert len(tmpVal) == 13


def test_statistics_byAccount():
    data = gcp_log_toolbox.pdFrame("./unit_test_logs/json_lines_small.json")
    tmpVal = gcp_log_toolbox.statistics_byAccount(data)
    assert len(tmpVal) == 5


def test_statistics_bySeverity():
    data = gcp_log_toolbox.pdFrame("./unit_test_logs/json_lines_small.json")
    tmpVal = gcp_log_toolbox.statistics_bySeverity(data)
    assert len(tmpVal) == 5


def test_convertTimeString():
    tmpVal = gcp_log_toolbox.convertTimeString("2019-07-22 20:04:31")
    expectedVal = datetime.strptime("2019-07-22 20:04:31", '%Y-%m-%d %H:%M:%S')
    assert tmpVal == expectedVal


def test_getTimeDeltas():
    dateTimeVal = datetime.strptime("2019-07-22 20:04:31", '%Y-%m-%d %H:%M:%S')
    startTimeVal, endTimeVal = gcp_log_toolbox.getTimeDeltas(dateTimeVal, 1)
    expectedStart = datetime.strptime("2019-07-22 20:04:01", '%Y-%m-%d %H:%M:%S')
    expectedEnd = datetime.strptime("2019-07-22 20:05:01", '%Y-%m-%d %H:%M:%S')
    assert startTimeVal == expectedStart
    assert endTimeVal == expectedEnd
    assert startTimeVal < endTimeVal


def test_getFileListing():
    recursiveList = gcp_log_toolbox.getFileListing("./unit_test_logs/cloud_storage_sink/*.json", True)
    nonRecursiveList = gcp_log_toolbox.getFileListing("./unit_test_logs/cloud_storage_sink/cloudaudit.googleapis.com/activity/2019/06/16/*.json", False)
    assert len(recursiveList) == 36
    assert len(nonRecursiveList) == 5

# I don't have a test GCP account to do unit tests against yet. I will set this up.


def test_parse_filters():
    filterString = "severity=NOTICE,protoPayload.authenticationInfo.principalEmail=test@testdomain.com"
    testVal = gcp_log_toolbox.parseFilters("include", filterString)
    assert testVal[0][0] == "severity"
    assert testVal[0][1] == "NOTICE"
    assert testVal[1][0] == "protoPayload.authenticationInfo.principalEmail"
    assert testVal[1][1] == "test@testdomain.com"


def test_filterLog():
    gcp_log_toolbox.filterLog("./unit_test_logs/json_lines_small.json",
                                True,
                                "./unit_test_logs/filterLogTest.json",
                                "severity=NOTICE,protoPayload.authenticationInfo.principalEmail=test@testdomain.com",
                                "include")
    with open("./unit_test_logs/filterLogTest.json") as f:
        content = f.readlines()
        f.close()

    os.remove("./unit_test_logs/filterLogTest.json")
    assert len(content) == 232


def test_gcloudformatter():
    gcp_log_toolbox.gcloudFormatter("./unit_test_logs/gcloud_array_small.json", "./unit_test_logs/gcloudformatter_tmp.json")
    with open("./unit_test_logs/gcloudformatter_tmp.json") as f:
        content = f.readlines()

    os.remove("./unit_test_logs/gcloudformatter_tmp.json")
    assert len(content) == 555

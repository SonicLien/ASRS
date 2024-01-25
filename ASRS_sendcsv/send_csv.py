import requests
import configparser
# import pandas as pd
import logging
import logging.handlers as handlers
from datetime import datetime
import os
import csv

alarmCount = 0

# get config
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
apiCfg = config['WEBAPI']
tornadoCfg = config['TORNADO']
csvFile = config['SOURCE']['CsvPath']
csvTFile = config['TARGET']['CsvTPath']
logFile = config['LOG']['Path']
retention = config['LOG']['Retention']

# api config
apiUrl = apiCfg['url']  # polling device api URL

# tornado config
url = tornadoCfg['url']  # tornado service URL
torn_body = {}  # parameters for tornado
url = tornadoCfg['url']
torn_body['desc'] = ''
torn_body['api_key'] = tornadoCfg['ApiKey']
torn_body['corp_id'] = tornadoCfg['CorpId']
torn_body['jobCat'] = tornadoCfg['JobCat']
torn_body['scene'] = tornadoCfg['Scene']
torn_body['tag'] = tornadoCfg['Tag']


def main():
    try:
        global alarmCount
        logger.info("============================")
        logger.info("Start")
        checkResetCount()  # reset Count limit if new day
        # check mode
        L1modeConfig = [0, 120, 106.0]  # [ID,DB,DBX] for mode
        L2modeConfig = [0, 220, 106.0]  # [ID,DB,DBX] for mode
        L3modeConfig = [0, 320, 106.0]  # [ID,DB,DBX] for mode
        L4modeConfig = [0, 420, 106.0]  # [ID,DB,DBX] for mode
        L5modeConfig = [0, 520, 106.0]  # [ID,DB,DBX] for mode
        L6modeConfig = [0, 620, 106.0]  # [ID,DB,DBX] for mode
        L1maintStatus = callApi(L1modeConfig)
        L2maintStatus = callApi(L2modeConfig)
        L3maintStatus = callApi(L3modeConfig)
        L4maintStatus = callApi(L4modeConfig)
        L5maintStatus = callApi(L5modeConfig)
        L6maintStatus = callApi(L6modeConfig)  # Call web api function and check if Maint Mode

        # Set init mode
        L1mode = 'O1'
        L2mode = 'O2'
        L3mode = 'O3'
        L4mode = 'O4'
        L5mode = 'O5'
        L6mode = 'O6'
        CommonMode = 'OC'

        if L1maintStatus == "1":
            L1mode = 'M1'
        elif L1maintStatus == 'Fail':
            logger.info("Fail to get L1 Mode status")
            exit()
        logger.info("L1 Mode: " + L1mode)
        if L2maintStatus == "1":
            L2mode = 'M2'
        elif L2maintStatus == 'Fail':
            logger.info("Fail to get L2 Mode status")
            exit()
        logger.info("L2 Mode: " + L2mode)
        if L3maintStatus == "1":
            L3mode = 'M3'
        elif L3maintStatus == 'Fail':
            logger.info("Fail to get L3 Mode status")
            exit()
        logger.info("L3 Mode: " + L3mode)
        if L4maintStatus == "1":
            L4mode = 'M4'
        elif L4maintStatus == 'Fail':
            logger.info("Fail to get L4 Mode status")
            exit()
        logger.info("L4 Mode: " + L4mode)
        if L5maintStatus == "1":
            L5mode = 'M5'
        elif L5maintStatus == 'Fail':
            logger.info("Fail to get L5 Mode status")
            exit()
        logger.info("L5 Mode: " + L5mode)
        if L6maintStatus == "1":
            L6mode = 'M6'
        elif L6maintStatus == 'Fail':
            logger.info("Fail to get L6 Mode status")
            exit()
        logger.info("L6 Mode: " + L6mode)

        # dfCsvTemp = pd.read_csv(csvFile)  # read csv
        # dfConfigList = dfCsvTemp.values
        dfConfigList = list()
        with open(csvFile, 'r') as f:
            for line in f.readlines():
                dfConfigList.append(line.strip().split(','))
        dfConfigList = dfConfigList[1:]
        # logger.info('dfConfigList: ' + str(dfConfigList))
        for alarmConfig in dfConfigList:  # Loop through alarm config list
            if alarmConfig[3] == L1mode or alarmConfig[3] == L2mode or alarmConfig[3] == L3mode or alarmConfig[3] == L4mode or alarmConfig[3] == L5mode or alarmConfig[3] == L6mode or alarmConfig[3] == CommonMode:  # Check if Alarm matches mode, Maintenance or Operation
                flag = callApi(alarmConfig)  # Call web api function and check if config item have alarm
                if flag == "1":
                    desc = str(datetime.now().strftime("%Y%m%d %H%M%S%f")) + ',' + str(alarmConfig[4])
                    csv_body = desc  # content that will display in tornado email
                    logger.info(desc)
                    # if alarmCount < int(tornadoCfg['LimitedCount']): #Check condition for limited number tornado alarm
                    # logger.info("sendTornadoLimited")
                    # sendTornado(True)
                    # else:
                    # logger.info("Alarm Count Limit reach: " + str(alarmCount))
                    logger.info("Write to CSV")
                    csvDate = os.path.join(csvTFile, str(datetime.now().strftime('%Y%m%d')) + '.csv')
                    with open(csvDate, 'a', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile)
                        csvwriter.writerow(csv_body.split(','))
                    # alarmCount += 1
                    # with open('alarmcount.txt', 'w') as f: # Update the number of Alarm for the day
                    # f.write(str(alarmCount))
        logger.info("End")

    except Exception as ex:
        logger.error('[main] ' + str(ex))


def checkResetCount():
    try:
        # get alarm count
        global alarmCount
        ac = open("alarmcount.txt", "r")
        alarmCount = int(ac.readlines()[0])
        ad = open("alarmdate.txt", "r")
        countDate = int(ad.readlines()[0])
        logger.info(countDate)
        currDate = datetime.now().strftime('%Y%m%d')
        if currDate != str(countDate):  # Check if it is a new day and reset alarm count to 0
            with open('alarmcount.txt', 'w') as f:  # Update the number of Alarm for the day
                f.write('0')
            with open('alarmdate.txt', 'w') as f:  # Update Current date
                f.write(currDate)
            logger.info("reset count")
            alarmCount = 0
    except Exception as ex:
        logger.error('[checkResetCount] ' + str(ex))


def callApi(alarmConfig):  # get data from polling device
    try:
        global apiUrl
        argument = {}
        argument['memory_address'] = 'DB' + str(alarmConfig[1]) + '.' + 'DBX' + str(alarmConfig[2])
        argument['number_of_items'] = 1
        argument['network_no'] = 0
        argument['node_no'] = 0
        # sample url = http://192.168.20.50:8080/api/getPLCDeviceDataBatch?argument={“memory_address”:”DB80.DBX0.0”, “number_of_items”:1, “network_no”:0, “node_no”:0}
        newUrl = apiUrl + "argument='" + str(argument) + "'"
        # result = requests.get(apiUrl,params='argument='+ str(argument),timeout=5) # get data
        result = requests.get(newUrl, timeout=5)
        flag = str(result.json()['rtn_data'])
        return flag
    except Exception as ex:
        logger.error('[callApi] ' + str(ex))
        return 'Fail'


# def sendTornado(isLimited):
#     try:
#         if isLimited:
#             torn_body['tag'] = tornadoCfg['LimitedTag']
#         else:
#             torn_body['tag'] = tornadoCfg['Tag']
#         headers = {'content-type': 'application/json'}
#         response = requests.post(url, data=str(torn_body), headers=headers)  # Send to Tornado
#         logger.info('Tornado Response: ' + str(response))  # 200 means successful
#     except Exception as ex:
#         logger.error('[sendTornado] ' + str(ex))
#
#
# def sendCSV(isLimited):
#     # try:
#     # if isLimited:
#     # torn_body['tag'] = tornadoCfg['LimitedTag']
#     # else:
#     # torn_body['tag'] = tornadoCfg['Tag']
#     headers = {'content-type': 'application/json'}
#     response = requests.post(url, data=str(torn_body), headers=headers)  # Send to Tornado
#     # logger.info('CSV Response: ' + str(response)) # 200 means successful
#
#
# # except Exception as ex:
# # logger.error('[sendCSV] ' + str(ex))

# log initialise
logger = logging.getLogger('my_app')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logHandler = handlers.TimedRotatingFileHandler(logFile + 'etl.log', when='midnight', backupCount=int(retention))
logHandler.setLevel(logging.INFO)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

main()  # call main function



import os
import errno
import json
import shutil
from datetime import datetime
from time import time, sleep

import tushare as ts
import tarfile

config = open('config.json')
setting = json.load(config)

SYMBOLS = setting['SYMBOLS']

#----------------------------------------------------------------------
def downloadTodayData():
    """download all the txn happened today of SYMBOLS"""

    cwd = os.getcwd()
    stampStart = time()

    # step 1. folder of today
    shutil.rmtree('tmp', ignore_errors=True) 
    mkdir_p('tmp')
    os.chdir('tmp')
    day_asof = datetime.today().strftime('%Y%m%d')
    path = "ts" + day_asof
    mkdir_p('./' + path)

    # step 1.1 index data
    for i in range(1,16):
        try:
            ts.get_index().to_csv(path+"/index_ov.csv", header=True, index=False, encoding='utf-8')
            break
        except IOError as exc:  # Python >2.5
            sleep(30*i)
            pass

    # step 1.2 all ticks' day overview
    for i in range(1,16):
        try:
            ts.get_today_all().to_csv(path+"/all.csv", header=True, index=False, encoding='utf-8')
            break
        except IOError as exc:  # Python >2.5
            sleep(30*i)
            pass

    # step 2. create the folder of the date
    mkdir_p(path + "/txn")
    symbolsToDownload =SYMBOLS
    while len(symbolsToDownload) >0:
        symbolsToRetry =[]
        for symbol in symbolsToDownload:
            if not downloadTxnsBySymbol(symbol, folder=path +"/txn"):
                symbolsToRetry.append(symbol)
        symbolsToDownload = symbolsToRetry


    # step 3. create the folder of the date
    mkdir_p(path + "/sina")

    # step 4. compress the tar ball
    tar = tarfile.open(cwd +'/ts' + day_asof + '.tar.bz2', 'w:bz2')
    tar.add(path)
    tar.close()

    elapsed = (time() - stampStart) * 1000

    print("took %dmsec to download today's data" % elapsed)
    os.chdir(cwd)


#----------------------------------------------------------------------
def downloadTxnsBySymbol(symbol, folder):
    """download all the txn happened today"""
    try:
        print "\ndownloading %s" % symbol
        df = ts.get_today_ticks(symbol)
        df.to_csv(folder + "/" + symbol+'.csv', header=True, index=False, encoding='utf-8')
        return True
    except IOError as exc:  # Python >2.5
        print "\nfailed to download %s" % symbol
        sleep(10)
        pass

    return False


def mkdir_p(path):
    try:
        os.makedirs(path, 0777)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

downloadTodayData()

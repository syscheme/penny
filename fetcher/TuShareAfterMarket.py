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
YIELDSEC =10
#----------------------------------------------------------------------
def downloadTodayData():
    """download all the txn happened today of SYMBOLS"""

    cwd = os.getcwd()
    stampStart = time()
    day_asof = datetime.today().strftime('%Y%m%d')

    mkdir_p('tmp')
    os.chdir('tmp')

    # step 0. backup previous downloading
    for _, subdirs, _ in os.walk(cwd +'/tmp'):
        for sd in subdirs:
            if 'ts' in sd :
                tar = tarfile.open(cwd + '/' + sd + '.tar.bz2', 'w:bz2')
                tar.add(sd)
                tar.close()
            if not day_asof in sd:
                shutil.rmtree(sd, ignore_errors=True)

    # step 1. folder of today
    path = "ts" + day_asof
    mkdir_p(os.getcwd() + '/' + path)

    # step 1.1 index data
    print "\ndownloading index_ov.csv"
    for i in range(1,16):
        try:
            ts.get_index().to_csv(path+"/index_ov.csv", header=True, index=False, encoding='utf-8')
            break
        except IOError as exc:  # Python >2.5
            sleep(YIELDSEC*i)
            pass

    # step 1.2 all ticks' day overview
    fsize =0
    try :
        fsize = os.stat(path +"/all.csv").st_size
    except :
        pass

    if fsize < 100 :
        print "\ndownloading all.csv"
        for i in range(1,16):
            try:
                ts.get_today_all().to_csv(path+"/all.csv", header=True, index=False, encoding='utf-8')
                break
            except IOError as exc:  # Python >2.5
                sleep(YIELDSEC*i)
                pass

    # step 2. create the folder of the date
    roundNo=0
    mkdir_p(path + "/txn")
    symbolsToDownload =SYMBOLS
    while len(symbolsToDownload) >0:
        roundNo = roundNo+1
        symbolsToRetry =[]
	print "\nround[%d] %d symbols to download\n" % (roundNo, len(symbolsToDownload))
        for symbol in symbolsToDownload:
            try :
                fsize =0
                fsize = os.stat(path +"/txn/" +symbol).st_size
            except :
                pass

            if fsize > 1000 :
                continue

            print "\nround[%d] downloading %s" % (roundNo, symbol)
            if not downloadTxnsBySymbol(symbol, folder=path +"/txn"):
                print "\nround[%d] failed to download %s" % (roundNo, symbol)
                symbolsToRetry.append(symbol)

        symbolsToDownload = symbolsToRetry


    # step 3. create the folder of the date
    mkdir_p(path + "/sina")

    # step 4. compress the tar ball
    tar = tarfile.open(cwd +'/ts' + day_asof + '.tar.bz2', 'w:bz2')
    tar.add(path)
    tar.close()
    shutil.rmtree(path, ignore_errors=True)

    elapsed = (time() - stampStart) * 1000

    print("took %dmsec to download today's data" % elapsed)
    os.chdir(cwd)


#----------------------------------------------------------------------
def downloadTxnsBySymbol(symbol, folder):
    """download all the txn happened today"""
    try:
        df = ts.get_today_ticks(symbol)
        df.to_csv(folder + "/" + symbol+'.csv', header=True, index=False, encoding='utf-8')
        return True
    except IOError as exc:  # Python >2.5
        sleep(YIELDSEC)
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

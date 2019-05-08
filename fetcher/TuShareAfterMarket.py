import os
import errno
import json
import shutil
from datetime import datetime
import time
import tushare as ts
import tarfile

config = open('config.json')
setting = json.load(config)

SYMBOLS = setting['SYMBOLS']
YIELDSEC =5
MAX_ROUNDS=20
#----------------------------------------------------------------------
def downloadTodayData():
    """download all the txn happened today of SYMBOLS"""

    cwd = os.getcwd()
    stampStart = time.time()
    day_asof = datetime.today().strftime('%Y%m%d')
    endOfDay = datetime.strptime(day_asof +' 23:50:00', '%Y%m%d %H:%M:%S')
    stampToStop = time.mktime(endOfDay.timetuple())

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
    print "\n" + day_asof +": downloading index_ov.csv"
    for i in range(1,MAX_ROUNDS):
        try:
            ts.get_index().to_csv(path+"/index_ov.csv", header=True, index=False, encoding='utf-8')
            break
        except IOError as exc:  # Python >2.5
            time.sleep(YIELDSEC*i)
            pass

    # step 1.2 all ticks' day overview
    fsize =0
    try :
        fsize = os.stat(path +"/all.csv").st_size
    except :
        pass

    if fsize < 100 :
        print "\n" + day_asof +": downloading all.csv"
        for i in range(1,MAX_ROUNDS*2):
            try:
                ts.get_today_all().to_csv(path+"/all.csv", header=True, index=False, encoding='utf-8')
                break
            except IOError as exc:  # Python >2.5
                time.sleep(YIELDSEC*i)
                pass

    # step 2. create the folder of the date
    mkdir_p(path + "/txn")
    symbolsToDownload =SYMBOLS

    for roundNo in range(1, MAX_ROUNDS) :
        if len(symbolsToDownload) <=0:
            break

        stampRoundStart = time.time()
        if stampRoundStart> stampToStop :
            break;

        symbolsToRetry =[]
        succeeded=[]
        stampRoundStart = time.time()
	print "\n" + day_asof +": round[%d] %d symbols to download" % (roundNo, len(symbolsToDownload))
        for symbol in symbolsToDownload:
            try :
                fsize =0
                fsize = os.stat(path +"/txn/" +symbol).st_size
            except :
                pass

            if fsize > 1000 :
                continue

            print "\n" + day_asof +": round[%d] downloading %s" % (roundNo, symbol)
            if downloadTxnsBySymbol(symbol, folder=path +"/txn"):
                succeeded.append(symbol)
            else:
                print "\n" + day_asof +": round[%d] failed to download %s, enque to retry" % (roundNo, symbol)
                symbolsToRetry.append(symbol)

        symbolsToDownload = symbolsToRetry
        print "\n" + day_asof +": round[%d] downloaded %s, took %dsec, %d symbols to retry" % (roundNo, succeeded, (time.time() - stampRoundStart), len(symbolsToDownload))

    # step 3. create the folder of the date
    mkdir_p(path + "/sina")

    # step 4. compress the tar ball
    tar = tarfile.open(cwd +'/ts' + day_asof + '.tar.bz2', 'w:bz2')
    tar.add(path)
    tar.close()
    shutil.rmtree(path, ignore_errors=True)
    print "\n" + day_asof +": done, took %dsec to download today's data" % (time.time() - stampStart)
    os.chdir(cwd)


#----------------------------------------------------------------------
def downloadTxnsBySymbol(symbol, folder):
    """download all the txn happened today"""
    try:
        df = ts.get_today_ticks(symbol)
        df.to_csv(folder + "/" + symbol+'.csv', header=True, index=False, encoding='utf-8')
        return True
    except IOError as exc:  # Python >2.5
        time.sleep(YIELDSEC)
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

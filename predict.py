import MySQLdb
# import cPickle
import numpy as np
import time
import csv
from sklearn.externals import joblib
import pdb


def ConnectMysql():
    conn = MySQLdb.connect(host='localhost', user='root', passwd='lx525149',
                           port=3306, db='ijcai')
    return conn


if __name__ == '__main__':

    conn = ConnectMysql()
    start = time.time()
    tablePre = 'ULM1112'

    modelFilePath = '../modleFileFeat14/'
    modelFileName = 'GBDT100Dec10M1011.pkl'
    print 'modelFileName: ', modelFileName
    model = joblib.load(modelFilePath + modelFileName)

    cur = conn.cursor()
    cur.execute("select count(*) from %s" % tablePre)
    ulmNum = cur.fetchall()[0][0]
    many = 1000000
    fetchTimes = int(ulmNum / many) + 1
    ULMDic = {}
    submitNum = 0
    for i in range(fetchTimes):
        print i
        offset = i * many
        ULM = []
        Xpre = []
        sql = "SELECT * FROM %s limit %d, %d" % (tablePre, offset, many)
        execute = cur.execute(sql)
        for data in cur.fetchall():
            ULM.append(data[0:3])
            Xpre.append(data[3:])
        ULM = np.array(ULM)
        Xpre = np.array(Xpre)

        Ypre = model.predict_proba(Xpre)[:, 1]
        # prob = 0.165
        # ULM = ULM[Ypre > prob]

        # submitNum += len(ULM)
        j = 0
        for ulm in ULM:
            if (ulm[0], ulm[1]) not in ULMDic:
                ULMDic[(ulm[0], ulm[1])] = [(ulm[2], Ypre[j])]
            else:
                ULMDic[(ulm[0], ulm[1])].append((ulm[2], Ypre[j]))
            j += 1

    cur.close()
    conn.close()
    end = time.time()
    print "Predicted time is: ", end - start

    submitFilePath = '../submitFile/'
    submitName = modelFileName[0:-4] + '.csv'
    f = open(submitFilePath + submitName, 'w')
    writer = csv.writer(f)
    submitNum = 0
    for key in ULMDic:
        ul = [key[0], key[1]]
        merchant = ULMDic[key]
        merchant.sort(key=lambda x: x[1], reverse=True)
        lenMerchant = len(merchant)
        merSubmit = []
        n = 0
        # pdb.set_trace()
        for mer in merchant:
            if n < 1:
                merSubmit = [mer[0]]
            if n >= 2 and mer[1] > 0.25:
                merSubmit.append(mer[0])
            n += 1
            # if n > 4:
            #     break

        submitNum += len(merSubmit)
        merSubmit = ':'.join(merSubmit)
        ul.extend([merSubmit])
        writer.writerow(ul)
    f.close()
    print "submission number: ", submitNum

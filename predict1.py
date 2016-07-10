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

    modelFilePath = '../modelFileUL1F12/'
    modelFileName = 'GBDT100Dec8M1011UL1F12.pkl'
    print 'modelFileName: ', modelFileName
    model = joblib.load(modelFilePath + modelFileName)

    cur = conn.cursor()
    cur.execute("select count(*) from %s" % tablePre)
    ulmNum = cur.fetchall()[0][0]
    many = 1000000
    fetchTimes = int(ulmNum / many) + 1
    ULMDic = {}
    submitNum = 0
    ULMProb = []
    for i in range(fetchTimes):
        print i
        offset = i * many
        ULM = []
        Xpre = []
        sql = "SELECT * FROM %s limit %d, %d" % (tablePre, offset, many)
        execute = cur.execute(sql)
        for data in cur.fetchall():
            ULM.append(data[0:3])
            Xpre.append(data[3:15])
        ULM = np.array(ULM)
        Xpre = np.array(Xpre)

        Ypre = model.predict_proba(Xpre)[:, 1]

        prob = 0.15
        ULM = ULM[Ypre > prob]
        Ypre = Ypre[Ypre > prob]
        ULMProb.extend(zip(ULM, Ypre))
        # pdb.set_trace()
    ULMProb.sort(key=lambda x: x[1], reverse=True)
    submitNum = 1519547
    ULMProb1 = ULMProb[0: submitNum]
    submitNum = len(ULMProb1)
    for ulmProb in ULMProb1:
        ulm = ulmProb[0]
        if (ulm[0], ulm[1]) not in ULMDic:
            ULMDic[(ulm[0], ulm[1])] = [ulm[2]]
        else:
            ULMDic[(ulm[0], ulm[1])].append(ulm[2])

    cur.close()
    conn.close()
    end = time.time()
    print "submission number: ", submitNum
    print "Predicted time is: ", end - start

    submitFilePath = '../submitFile/'
    submitName = modelFileName[0:-4] + 'Num' + str(submitNum) + '.csv'
    f = open(submitFilePath + submitName, 'w')
    writer = csv.writer(f)
    for key in ULMDic:
        array = [key[0], key[1]]
        Merchant = ':'.join(ULMDic[key])
        array.extend([Merchant])
        writer.writerow(array)
    f.close()

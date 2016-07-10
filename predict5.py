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


def QueryMerBudget():
    conn = ConnectMysql()
    cur = conn.cursor()
    sql = "SELECT Merchant_id, Budget FROM merchant_info"

    cur.execute(sql)

    MerBudgetDic = cur.fetchall()
    MerBudgetDic = dict(MerBudgetDic)

    cur.close()
    conn.close()
    return MerBudgetDic

if __name__ == '__main__':

    conn = ConnectMysql()
    start = time.time()
    tablePre = 'ULM1112'

    modelFilePath = '../modelFileUL1/'
    modelFileName = 'GBDT100Dec8M1011UL1F12.pkl'
    print 'modelFileName: ', modelFileName
    model = joblib.load(modelFilePath + modelFileName)

    cur = conn.cursor()
    cur.execute("select count(*) from %s" % tablePre)
    ulmNum = cur.fetchall()[0][0]
    many = 2000000
    fetchTimes = int(ulmNum / many) + 1
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
        # prob = 0.165
        # ULM = ULM[Ypre > prob]

        # submitNum += len(ULM)
        # prob = 0.12
        # ULM = ULM[Ypre > prob]
        # Ypre = Ypre[Ypre > prob]
        ULMProb.extend(zip(ULM, Ypre))
        # pdb.set_trace()
    cur.close()
    conn.close()

    ULDic = {}
    for ulmProb in ULMProb:
        ulm = ulmProb[0]
        if (ulm[0], ulm[1]) not in ULDic:
            ULDic[(ulm[0], ulm[1])] = [[ulm, ulmProb[1]]]
        else:
            ULDic[(ulm[0], ulm[1])].append([ulm, ulmProb[1]])
    ULMProb = []
    for key in ULDic:
        ULDic[key].sort(key=lambda x: x[1], reverse=True)
        ULDic[key] = ULDic[key][0: 10]
        sumULProb = sum(map(lambda x: x[1], ULDic[key]))
        for i in range(len(ULDic[key])):
            ULDic[key][i][1] = ULDic[key][i][1] * 1.0 / sumULProb
        ULMProb.extend(ULDic[key])

    MerBudgetDic = QueryMerBudget()
    MerDic = {}
    for ulmProb in ULMProb:
        ulm = ulmProb[0]
        if ulm[2] not in MerDic:
            MerDic[ulm[2]] = [ulmProb]
        else:
            MerDic[ulm[2]].append(ulmProb)
    # pdb.set_trace()
    ULMProbBudget = []
    for key in MerBudgetDic:
        mer = key
        budget = int(int(MerBudgetDic[key]) * 1.8)
        if mer not in MerDic:
            continue
        MerDic[mer].sort(key=lambda x: x[1], reverse=True)
        ULMProbBudget.extend(MerDic[mer][0:budget])

    ULMSubmitDic = {}
    for ulmProb in ULMProbBudget:
        ulm = ulmProb[0]
        if (ulm[0], ulm[1]) not in ULMSubmitDic:
            ULMSubmitDic[(ulm[0], ulm[1])] = [ulm[2]]
        else:
            ULMSubmitDic[(ulm[0], ulm[1])].append(ulm[2])

    end = time.time()
    print "Predicted time is: ", end - start

    submitNum = len(ULMProbBudget)
    submitFilePath = '../submitFile/'
    submitName = modelFileName[0:-4] + 'NumB5W' + str(submitNum) + '.csv'
    f = open(submitFilePath + submitName, 'w')
    writer = csv.writer(f)
    for key in ULMSubmitDic:
        array = [key[0], key[1]]
        Merchant = ':'.join(ULMSubmitDic[key])
        array.extend([Merchant])
        writer.writerow(array)
    f.close()
    print "submission number: ", submitNum

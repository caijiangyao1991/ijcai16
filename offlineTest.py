import MySQLdb
# import cPickle
import numpy as np
import pdb
import time
import csv
from sklearn.externals import joblib


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
    tableTest = 'ULM1011'
    start = time.time()
    modelFilePath = '../modelFileUL1/'
    modelFileName = 'GBDT100Dec8M0910UL1F12.pkl'
    print 'modelFileName: ', modelFileName
    model = joblib.load(modelFilePath + modelFileName)
    cur = conn.cursor()
    cur.execute('''
                SELECT COUNT(DISTINCT User_id, Location_id, Merchant_id)
                FROM koubei_train
                WHERE Time_Stamp BETWEEN '2015-11-01' and '2015-11-30'
                ''')

    TruePosLabelNum = cur.fetchall()[0][0]

    cur.execute("select count(*) from %s" % tableTest)
    ulmNum = cur.fetchall()[0][0]
    many = 3000000
    fetchTimes = int(ulmNum / many) + 1
    ULMDic = {}
    TPNum = 0
    allPosNum = 0
    submitNum = 0
    pYandY = []
    ULMProb = []
    ULMReal = []
    for i in range(fetchTimes):
        print i
        offset = i * many
        Xtest = []
        Ytest = []
        ULM = []
        sql = "SELECT * FROM %s limit %d, %d" % (tableTest, offset, many)
        execute = cur.execute(sql)
        for data in cur.fetchall():
            ULM.append(data[0:3])
            Xtest.append(data[4:16])
            Ytest.append(data[3])
        ULM = np.array(ULM)
        Xtest = np.array(Xtest)
        Ytest = np.array(Ytest)

        # pdb.set_trace()
        pYtest = model.predict_proba(Xtest)[:, 1]
        prob = 0.5
        pULM = ULM[pYtest > prob]
        pYtest = pYtest[pYtest > prob]
        rULM = ULM[Ytest == 1]
        # rYtest = Ytest[Ytest == 1]
        ULMProb.extend(zip(pULM, pYtest))
        ULMReal.extend(rULM)
    cur.close()
    conn.close()

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
        budget = int(int(MerBudgetDic[key]) / 3 * 5)
        if mer not in MerDic:
            continue
        MerDic[mer].sort(key=lambda x: x[1], reverse=True)
        ULMProbBudget.extend(MerDic[mer][0:budget])
    ULMPre1 = map(lambda x: tuple(x[0]), ULMProbBudget)
    ULMReal1 = map(lambda x: tuple(x), ULMReal)
    inter = set(ULMPre1) & set(ULMReal1)

    Precise = 1.0 * len(inter) / len(ULMPre1)
    Recall = 1.0 * len(inter) / len(ULMReal1)
    F1 = 2.0 * Precise * Recall / (Precise + Recall)
    print 'F1/P/R %f/%f/%f\n' % (F1, Precise, Recall)

    end = time.time()
    print "offline test time is: ", end - start

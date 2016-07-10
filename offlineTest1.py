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
    many = 2000000
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
        prob = 0.2
        pULM = ULM[pYtest > prob]
        pYtest = pYtest[pYtest > prob]
        rULM = ULM[Ytest == 1]
        # rYtest = Ytest[Ytest == 1]
        ULMProb.extend(zip(pULM, pYtest))
        ULMReal.extend(rULM)
    cur.close()
    conn.close()

    MerBudgetDic = QueryMerBudget()
    MerProbDic = {}
    for ulmProb in ULMProb:
        ulm = ulmProb[0]
        if ulm[2] not in MerProbDic:
            MerProbDic[ulm[2]] = [ulmProb]
        else:
            MerProbDic[ulm[2]].append(ulmProb)

    MerRealDic = {}
    for ulm in ULMReal:
        if ulm[2] not in MerRealDic:
            MerRealDic[ulm[2]] = [ulm]
        else:
            MerRealDic[ulm[2]].append(ulm)
    # pdb.set_trace()
    # ULMProbBudget = []
    PRNumera = 0
    PDenom = 0
    RDenom = 0
    overBudNum = 0
    for key in MerBudgetDic:
        mer = key
        realBudget = int(int(MerBudgetDic[key]) / 2.5)
        bigBudget = realBudget * 2
        if (mer not in MerProbDic) and (mer not in MerRealDic):
            continue
        elif mer not in MerProbDic:
            ULMRealPerM = MerRealDic[mer]
            # ULMRealPerM = map(lambda x: tuple(x), ULMRealPerM)
            if len(ULMRealPerM) > realBudget:
                overBudNum += 1
            RDenom += min(len(ULMRealPerM), realBudget)
        elif mer not in MerRealDic:
            MerProbDic[mer].sort(key=lambda x: x[1], reverse=True)
            ULMPrePerM = MerProbDic[mer][0:bigBudget]
            # ULMPrePerM = map(lambda x: tuple(x[0]), ULMPrePerM)
            PDenom += len(ULMPrePerM)
        else:
            MerProbDic[mer].sort(key=lambda x: x[1], reverse=True)
            ULMProbBudget = MerProbDic[mer][0:bigBudget]
            ULMPrePerM = map(lambda x: tuple(x[0]), ULMProbBudget)
            ULMRealPerM = MerRealDic[mer]
            ULMRealPerM = map(lambda x: tuple(x), ULMRealPerM)
            inter = set(ULMPrePerM) & set(ULMRealPerM)
            PRNumera += min(len(inter), realBudget)
            PDenom += len(ULMPrePerM)
            RDenom += min(len(ULMRealPerM), realBudget)
            if len(ULMRealPerM) > realBudget:
                overBudNum += 1

    Precise = 1.0 * PRNumera / PDenom
    Recall = 1.0 * PRNumera / RDenom
    F1 = 2.0 * Precise * Recall / (Precise + Recall)
    print 'F1/P/R %f/%f/%f\n' % (F1, Precise, Recall)
    print 'over budget number:', overBudNum
    end = time.time()
    print "offline test time is: ", end - start

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
    start = time.time()
    ULDic = {}
    with open('GBDT100Dec8M1011UL1F12.csv', 'r') as fp:
        for line in fp:
            ulm = line.strip().split(',')
            ulm[3] = float(ulm[3])
            if (ulm[0], ulm[1]) not in ULDic:
                ULDic[(ulm[0], ulm[1])] = [ulm]
            else:
                ULDic[(ulm[0], ulm[1])].append(ulm)
    ULMProb = []
    for key in ULDic:
        ULDic[key].sort(key=lambda x: x[3], reverse=True)
        ULDic[key] = ULDic[key][0: 4]
        sumULProb = sum(map(lambda x: x[3], ULDic[key]))
        for i in range(len(ULDic[key])):
            ULDic[key][i][3] = ULDic[key][i][3] * 1.0 / sumULProb
        ULMProb.extend(ULDic[key])
    # pdb.set_trace()
    MerBudgetDic = QueryMerBudget()
    MerDic = {}
    for ulm in ULMProb:
        if ulm[2] not in MerDic:
            MerDic[ulm[2]] = [ulm]
        else:
            MerDic[ulm[2]].append(ulm)
    # pdb.set_trace()
    ULMProbBudget = []
    for key in MerBudgetDic:
        mer = key
        budget = int(int(MerBudgetDic[key]) * 2)
        if mer not in MerDic:
            continue
        MerDic[mer].sort(key=lambda x: x[3], reverse=True)
        ULMProbBudget.extend(MerDic[mer][0:budget])

    ULMSubmitDic = {}
    for ulm in ULMProbBudget:
        if (ulm[0], ulm[1]) not in ULMSubmitDic:
            ULMSubmitDic[(ulm[0], ulm[1])] = [ulm[2]]
        else:
            ULMSubmitDic[(ulm[0], ulm[1])].append(ulm[2])

    end = time.time()
    print "Predicted time is: ", end - start

    submitNum = len(ULMProbBudget)
    submitFilePath = '../submitFile/'
    submitName = 'Top6Norm' + str(submitNum) + '.csv'
    f = open(submitFilePath + submitName, 'w')
    writer = csv.writer(f)
    for key in ULMSubmitDic:
        array = [key[0], key[1]]
        Merchant = ':'.join(ULMSubmitDic[key])
        array.extend([Merchant])
        writer.writerow(array)
    f.close()
    print "submission number: ", submitNum

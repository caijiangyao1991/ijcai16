# !usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
# import cPickle
import numpy as np
import pdb
import time
import csv
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn import cross_validation
from sklearn.externals import joblib
from sklearn.learning_curve import learning_curve
from sklearn import metrics
from unbalanced_dataset import UnderSampler, ClusterCentroids


def ConnectMysql():
    conn = MySQLdb.connect(host='localhost', user='root', passwd='lx525149',
                           port=3306, db='ijcai')
    return conn


def GetXY(tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    sql = "SELECT * FROM %s" % tableName

    cur.execute(sql)

    X = []
    Y = []
    for data in cur.fetchall():
        X.append(data[4:16])
        Y.append(data[3])

    X = np.array(X)
    Y = np.array(Y)

    cur.close()
    conn.close()
    return X, Y


if __name__ == '__main__':

    tableTrain = 'ULM1011'

    modelFilePath = '../modelFileUL1/'
    modelFileName = 'GBDT300Dec8M1011UL1F12.pkl'
    print 'modelFileName: ', modelFileName

    start = time.time()
    Xtrain, Ytrain = GetXY(tableTrain)
    end = time.time()
    print "Get Train XY Over: ", end - start

    # model = LogisticRegression()
    # model = RandomForestClassifier(n_estimators=200)
    model = GradientBoostingClassifier(n_estimators=300)
    # model = AdaBoostClassifier()

    start = time.time()

    US = UnderSampler(ratio=8.)
    # US = ClusterCentroids(ratio=5.)
    Xtrain1, Ytrain1 = US.fit_transform(Xtrain, Ytrain)
    end = time.time()
    print "Data decimation time: ", end - start

    start = time.time()
    model.fit(Xtrain1, Ytrain1)
    joblib.dump(model, modelFilePath + modelFileName)
    end = time.time()
    print "model train time: ", end - start
    # print metrics.classification_report(model.predict(Xtrain), Ytrain)
    pYtrain = model.predict_proba(Xtrain)[:, 1]
    pYtrain = map(lambda x: 1 if x > 0.4 else 0, pYtrain)
    submitNum = sum(pYtrain)
    allPosNum = sum(Ytrain)
    Yzip = zip(Ytrain, pYtrain)
    TPNum = Yzip.count((1, 1))
    Precise = 1.0 * TPNum / submitNum
    Recall = 1.0 * TPNum / allPosNum
    F1 = 2.0 * Precise * Recall / (Precise + Recall)
    # print 'submitNum of ulmNum Ratio= %f' % Ratio
    print 'F1/P/R %f/%f/%f\n' % (F1, Precise, Recall)

# !usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time


def ConnectMysql():
    conn = MySQLdb.connect(host='localhost', user='root', passwd='lx525149',
                           port=3306, db='ijcai')
    return conn


# 统计训练集的样本, (U, L, M)
def QueryULM(time_start, time_end, time_start_label, time_end_label, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    # cur.execute("DROP TABLE iF EXISTS %s" % tableName)

    sql = '''
          CREATE TABLE %s AS
          SELECT DISTINCT A1.User_id, A1.Location_id, A2.Merchant_id
          FROM (SELECT DISTINCT User_id, Location_id FROM koubei_train
          WHERE Time_Stamp BETWEEN '%s' and '%s') AS A1,
          (SELECT DISTINCT Location_id, Merchant_id FROM koubei_train
          WHERE Time_Stamp between '%s' and '%s') AS A2
          WHERE A1.Location_id = A2.Location_id
          ''' % (tableName, time_start, time_end_label, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 提取训练集标签
def QueryLabel(time_start_label, time_end_label, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN Label INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT DISTINCT User_id,  Location_id, Merchant_id
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s') T2
          SET T1.Label = 1
          WHERE T1.User_id = T2.User_id AND
          T1.Location_id = T2.Location_id AND
          T1.Merchant_id =T2.Merchant_id
          ''' % (tableName, time_start_label, time_end_label)

    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 统计线上预测集的样本, (U, L, M)
def QueryPreULM(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    # cur.execute("DROP TABLE iF EXISTS %s" % tableName)

    sql = '''
          CREATE TABLE %s AS
          SELECT DISTINCT A1.User_id, A1.Location_id, A2.Merchant_id
          FROM (SELECT DISTINCT User_id, Location_id FROM koubei_test) AS A1,
          (SELECT DISTINCT Location_id, Merchant_id FROM koubei_train
          WHERE Time_Stamp between '%s' and '%s') AS A2
          WHERE A1.Location_id = A2.Location_id
          ''' % (tableName, time_start, time_end)

    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 1.查询用户线下购买次数：(U, BuyTimes)
def QueryUserBuyTimesOffline(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UserBuyoff INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, COUNT(*) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY User_id) T2
          SET T1.UserBuyoff = T2.Num
          WHERE T1.User_id = T2.User_id
          ''' % (tableName, time_start, time_end)

    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 2.查询每个位置L每家商店M的销售量：(L, M, Sales),LMSales
def QueryLocaMerchantSales(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN LMSales INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT Location_id, Merchant_id, count(*) AS Num
                         FROM  koubei_train
                         WHERE Time_Stamp between '%s' and '%s'
                         GROUP BY Location_id, Merchant_id) T2
          SET T1.LMSales = T2.Num
          WHERE T1.Location_id = T2.Location_id AND
          T1.Merchant_id = T2.Merchant_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 3.查询商店分布在几个地方，MerInLocNum
def QueryMerchantHasLocaNum(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN MerHasLocNum INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT Merchant_id, count(distinct Location_id) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY Merchant_id) T2
          SET T1.MerHasLocNum = T2.Num
          WHERE T1.Merchant_id = T2.Merchant_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 4.查询去过位置L的人数,LCountPeop,格式：{L: countPeop}
def QueryLocaCountPeople(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN LCountPeop INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT Location_id, count(*) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp between '%s' and '%s'
                         GROUP BY Location_id) T2
          SET T1.LCountPeop = T2.Num
          WHERE T1.Location_id = T2.Location_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 5.统计位置L的商店数,LocHasMerNum,格式{L: Num}
def QueryLocHasMerNum(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN LocHasMerNum INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT Location_id, COUNT(distinct Merchant_id) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY Location_id) T2
          SET T1.LocHasMerNum = T2.Num
          WHERE T1.Location_id = T2.Location_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 6.U在M购买次数,UMBuyTimes格式：{(U,M): Num}
def QueryUserMerBuyTimes(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UMBuyTimes INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, Merchant_id, count(*) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY User_id, Merchant_id) T2
          SET T1.UMBuyTimes = T2.Num
          WHERE T1.User_id = T2.User_id AND
          T1.Merchant_id = T2.Merchant_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 7.U去过L的次数,ULTimes,格式{(U, L): Num}
def QueryUserLocTimes(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN ULTimes INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, Location_id, count(*) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY User_id, Location_id) T2
          SET T1.ULTimes = T2.Num
          WHERE T1.User_id = T2.User_id AND
          T1.Location_id = T2.Location_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 8.U去过L处M的次数,ULMTimes,格式{(U, L, M): Num}
def QueryUserLocMerTimes(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN ULMTimes INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, Location_id, Merchant_id, count(*) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY User_id, Location_id, Merchant_id) T2
          SET T1.ULMTimes = T2.Num
          WHERE T1.User_id = T2.User_id AND
          T1.Location_id = T2.Location_id AND
          T1.Merchant_id =T2.Merchant_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 9.查询用户线上购买次数,UBuyTimesOn,格式{U: BuyTimesOn}
def QueryUserBuyTimesOnline(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UBuyTimesOn INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, count(*) AS Num
                         FROM taobao
                         WHERE Time_Stamp BETWEEN '%s' and '%s' AND
                         Online_Action_id = '1'
                         GROUP BY User_id) T2
          SET T1.UBuyTimesOn = T2.Num
          WHERE T1.User_id = T2.User_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 10.查询用户线上点击次数,UClickTimesOn,格式{U: ClickTimesOn}
def QueryUserClickTimesOnline(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UClickTimesOn INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, count(*) AS Num
                         FROM taobao
                         WHERE Time_Stamp BETWEEN '%s' and '%s' AND
                         Online_Action_id = '0'
                         GROUP BY User_id) T2
          SET T1.UClickTimesOn = T2.Num
          WHERE T1.User_id = T2.User_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 11.查询用户线上转化率,写入UConverRateOn
def QueryUserConverRateOnline(tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN ConverRateOn FLOAT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s
          SET ConverRateOn = UBuyTimesOn / UClickTimesOn
          WHERE UClickTimesOn != 0;
          ''' % (tableName)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 12.查询实体店M的销售量在本地区L所占的百分比，MSaleInLocRate
def QueryMerSaleInLocRate(tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN MSaleInLocRate FLOAT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s
          SET MSaleInLocRate = LMSales / LCountPeop
          WHERE LCountPeop != 0;
          ''' % (tableName)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 13.U购买过的不同线下商店数M,UBuyMNumOff
def QueryUserBuyMerNumOff(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UBuyMNumOff INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, COUNT(DISTINCT Merchant_id) AS Num
                         FROM koubei_train
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY User_id) T2
          SET T1.UBuyMNumOff = T2.Num
          WHERE T1.User_id = T2.User_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 14.U购买过的不同线上商店数Seller
def QueryUserBuySellerNumOn(time_start, time_end, tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UBuySellerNumOn INT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s T1, (SELECT User_id, COUNT( DISTINCT Seller_id) AS Num
                         FROM taobao
                         WHERE Time_Stamp BETWEEN '%s' and '%s'
                         GROUP BY User_id) T2
          SET T1.UBuySellerNumOn = T2.Num
          WHERE T1.User_id = T2.User_id
          ''' % (tableName, time_start, time_end)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 15.U在M购买的次数占总次数的比例
def QueryUserBuyMerRate(tableName):
    conn = ConnectMysql()
    cur = conn.cursor()
    cur.execute("ALTER TABLE %s ADD COLUMN UBuyMRate FLOAT DEFAULT 0" % tableName)

    sql = '''
          UPDATE %s
          SET UBuyMRate = UMBuyTimes / UserBuyoff
          WHERE UserBuyoff != 0;
          ''' % (tableName)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()


# 得到训练表
def GetTrainTable(time_start, time_end, time_start_label, time_end_label, tableName):

    startTotal = time.time()

    start = time.time()
    QueryULM(time_start, time_end, time_start_label, time_end_label, tableName)
    end = time.time()
    print "0.训练样本查找时间: ", end - start

    start = time.time()
    QueryLabel(time_start_label, time_end_label, tableName)
    end = time.time()
    print "0.训练样本标签查找时间: ", end - start

    start = time.time()
    QueryUserBuyTimesOffline(time_start, time_end, tableName)
    end = time.time()
    print "1.用户线下购买次数查找时间: ", end - start

    start = time.time()
    QueryLocaMerchantSales(time_start, time_end, tableName)
    end = time.time()
    print "2.每个位置每家商店的销售量查找时间: ", end - start

    start = time.time()
    QueryMerchantHasLocaNum(time_start, time_end, tableName)
    end = time.time()
    print "3.商店分布在几个地方查找时间: ", end - start

    start = time.time()
    QueryLocaCountPeople(time_start, time_end, tableName)
    end = time.time()
    print "4.去过位置L的人数查找时间: ", end - start

    start = time.time()
    QueryLocHasMerNum(time_start, time_end, tableName)
    end = time.time()
    print "5.位置L的商店数查找时间: ", end - start

    start = time.time()
    QueryUserMerBuyTimes(time_start, time_end, tableName)
    end = time.time()
    print "6.U在M购买次数查找时间: ", end - start

    start = time.time()
    QueryUserLocTimes(time_start, time_end, tableName)
    end = time.time()
    print "7.U去过L的次数查找时间: ", end - start

    start = time.time()
    QueryUserLocMerTimes(time_start, time_end, tableName)
    end = time.time()
    print "8.U去过L处M的次数查找时间: ", end - start

    start = time.time()
    QueryUserBuyTimesOnline(time_start, time_end, tableName)
    end = time.time()
    print "9.用户线上购买次数查找时间: ", end - start

    start = time.time()
    QueryUserClickTimesOnline(time_start, time_end, tableName)
    end = time.time()
    print "10.用户线上点击次数查找时间:", end - start

    start = time.time()
    QueryUserConverRateOnline(tableName)
    end = time.time()
    print "11.用户线上转化率查找时间:", end - start

    start = time.time()
    QueryMerSaleInLocRate(tableName)
    end = time.time()
    print "12.实体店M的销售量在本地区L所占的百分比查找时间:", end - start

    start = time.time()
    QueryUserBuyMerNumOff(time_start, time_end, tableName)
    end = time.time()
    print "13.U购买过的不同线下商店数M查找时间:", end - start

    start = time.time()
    QueryUserBuySellerNumOn(time_start, time_end, tableName)
    end = time.time()
    print "14.U购买过的不同线上商店数Seller查找时间:", end - start

    start = time.time()
    QueryUserBuyMerRate(tableName)
    end = time.time()
    print "15.U在M购买的次数占总次数的比例查找时间:", end - start

    endTotal = time.time()
    print "训练集创建总时间: ", endTotal - startTotal


# 得到预测表
def GetPreTable(time_start, time_end, tableName):
    startTotal = time.time()

    start = time.time()
    QueryPreULM(time_start, time_end, tableName)
    end = time.time()
    print "0.预测样本查找时间: ", end - start

    start = time.time()
    QueryUserBuyTimesOffline(time_start, time_end, tableName)
    end = time.time()
    print "1.用户线下购买次数查找时间: ", end - start

    start = time.time()
    QueryLocaMerchantSales(time_start, time_end, tableName)
    end = time.time()
    print "2.每个位置每家商店的销售量查找时间: ", end - start

    start = time.time()
    QueryMerchantHasLocaNum(time_start, time_end, tableName)
    end = time.time()
    print "3.商店分布在几个地方查找时间: ", end - start

    start = time.time()
    QueryLocaCountPeople(time_start, time_end, tableName)
    end = time.time()
    print "4.去过位置L的人数查找时间: ", end - start

    start = time.time()
    QueryLocHasMerNum(time_start, time_end, tableName)
    end = time.time()
    print "5.位置L的商店数查找时间: ", end - start

    start = time.time()
    QueryUserMerBuyTimes(time_start, time_end, tableName)
    end = time.time()
    print "6.U在M购买次数查找时间: ", end - start

    start = time.time()
    QueryUserLocTimes(time_start, time_end, tableName)
    end = time.time()
    print "7.U去过L的次数查找时间: ", end - start

    start = time.time()
    QueryUserLocMerTimes(time_start, time_end, tableName)
    end = time.time()
    print "8.U去过L处M的次数查找时间: ", end - start

    start = time.time()
    QueryUserBuyTimesOnline(time_start, time_end, tableName)
    end = time.time()
    print "9.查询用户线上购买次数查找时间: ", end - start

    start = time.time()
    QueryUserClickTimesOnline(time_start, time_end, tableName)
    end = time.time()
    print "10.查询用户线上点击次数查找时间:", end - start

    start = time.time()
    QueryUserConverRateOnline(tableName)
    end = time.time()
    print "11.用户线上转化率查找时间:", end - start

    start = time.time()
    QueryMerSaleInLocRate(tableName)
    end = time.time()
    print "12.实体店M的销售量在本地区L所占的百分比查找时间:", end - start

    start = time.time()
    QueryUserBuyMerNumOff(time_start, time_end, tableName)
    end = time.time()
    print "13.U购买过的不同线下商店数M查找时间:", end - start

    start = time.time()
    QueryUserBuySellerNumOn(time_start, time_end, tableName)
    end = time.time()
    print "14.U购买过的不同线上商店数Seller查找时间:", end - start

    start = time.time()
    QueryUserBuyMerRate(tableName)
    end = time.time()
    print "15.U在M购买的次数占总次数的比例查找时间:", end - start

    endTotal = time.time()
    print "预测集创建总时间: ", endTotal - startTotal


if __name__ == '__main__':

    # time_start = '2015-09-01'
    # time_end = '2015-09-30'
    # time_start_label = '2015-10-01'
    # time_end_label = '2015-10-31'
    # tableName = 'NULM0910'

    # start = time.time()
    # GetTrainTable(time_start, time_end, time_start_label, time_end_label, tableName)
    # end = time.time()
    # print "Get Train table 'ULM0910' time is :", end - start

    time_start = '2015-10-01'
    time_end = '2015-10-30'
    time_start_label = '2015-11-01'
    time_end_label = '2015-11-30'
    tableName = 'NULM1011'

    start = time.time()
    GetTrainTable(time_start, time_end, time_start_label, time_end_label, tableName)
    end = time.time()
    print "Get Train table 'NULM1011' time is :", end - start

    time_start = '2015-11-01'
    time_end = '2015-11-30'
    tablePre = 'ULM1112'

    start = time.time()
    GetPreTable(time_start, time_end, tablePre)
    end = time.time()
    print "Get Test table 'ULM1112' time is :", end - start

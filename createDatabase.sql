
DROP TABLE IF EXISTS taobao;

CREATE TABLE  taobao(
User_id VARCHAR(10),
Seller_id VARCHAR(6),
Item_id VARCHAR(8),
Category_id VARCHAR(2),
Online_Action_id VARCHAR(2),
Time_Stamp date
);

LOAD DATA INFILE '/home/lxin/machinelearning/tianchi/ijcai/datasets/ijcai2016_taobao'
INTO TABLE taobao
FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'
LINES TERMINATED BY '\n';


drop table IF EXISTS merchant_info;

 CREATE TABLE merchant_info(
 Merchant_id VARCHAR(10),
 Budget VARCHAR(5),
 Location_id_list VARCHAR(600)
 );

LOAD DATA LOCAL INFILE '/home/lxin/machinelearning/tianchi/ijcai/datasets/ijcai2016_merchant_info'   
INTO TABLE merchant_info    
FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'   
LINES TERMINATED BY '\n'; 


drop table IF EXISTS koubei_train;

 CREATE TABLE koubei_train(
 User_id VARCHAR(10),
 Merchant_id VARCHAR(5),
 Location_id VARCHAR(5),
 Time_Stamp date
 );

LOAD DATA LOCAL INFILE '/home/lxin/machinelearning/tianchi/ijcai/datasets/ijcai2016_koubei_train'   
INTO TABLE koubei_train    
FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'   
LINES TERMINATED BY '\n';


CREATE TABLE koubei_test(
User_id VARCHAR(10),
Location_id VARCHAR(5)
);

LOAD DATA LOCAL INFILE '/home/lxin/machinelearning/tianchi/ijcai/datasets/ijcai2016_koubei_test'   
INTO TABLE koubei_test    
FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'   
LINES TERMINATED BY '\n';

drop table IF EXISTS taobao;

 CREATE TABLE taobao(
 User_id VARCHAR(10),
 Seller_id VARCHAR(6),
 Item_id VARCHAR(8),
 Category_id VARCHAR(2),
 Online_Action_id VARCHAR(2),
 Time_Stamp date
 );

LOAD DATA LOCAL INFILE '/home/lxin/machinelearning/tianchi/ijcai/datasets/ijcai2016_taobao'   
INTO TABLE taobao    
FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' ESCAPED BY '\\'   
LINES TERMINATED BY '\n'; 

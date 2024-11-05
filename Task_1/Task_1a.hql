DROP TABLE bankdata;
DROP TABLE jobcount;

-- Create a table for the input data
CREATE TABLE bankdata (age BIGINT, job STRING, marital STRING, education STRING,
  default STRING, balance BIGINT, housing STRING, loan STRING, contact STRING,
  day BIGINT, month STRING, duration BIGINT, campaign BIGINT, pdays BIGINT,
  previous BIGINT, poutcome STRING, termdeposit STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank.csv' INTO TABLE bankdata;

-- TODO: *** Put your solution here ***
--Task 1a Create a table to count number of clients of each job category 
CREATE TABLE jobcount AS 
SELECT job, count(1) AS count
FROM bankdata
WHERE job NOT LIKE ""
GROUP BY job;


-- Dump the output to file
INSERT OVERWRITE LOCAL DIRECTORY './task1a-out/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
  SELECT * FROM jobcount;

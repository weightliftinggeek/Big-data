DROP TABLE twitterdata;
DROP TABLE highestcount;
DROP TABLE twittedmost;

-- Create a table for the input data
CREATE TABLE twitterdata (tokenType STRING, month STRING, count BIGINT,
  hashtagName STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/twitter.tsv' INTO TABLE twitterdata;

-- TODO: *** Put your solution here ***

CREATE TABLE highestcount AS
SELECT hashtagName, sum(count) AS count
FROM  twitterdata
GROUP BY hashtagName
Order BY count desc
LIMIT 1;
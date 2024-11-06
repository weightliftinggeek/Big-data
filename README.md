# Task 1: Analysing Bank Data
We will be doing some analytics on real data from a Portuguese banking institution1. The data is stored in a semicolon (“;”) delimited format  
The data is supplied with the assignment at the following locations:  
![image](https://github.com/user-attachments/assets/b3b23317-8fe7-4a4e-9c14-03b52c06a726)  
The data has the following attributes  
![image](https://github.com/user-attachments/assets/c2f991f9-6860-46e0-ab5d-afcf5e5f0bba)  
Bank data source: https://archive.ics.uci.edu/dataset/222/bank+marketing
## 1a
[Hive] Report the number of clients of each job category. Write the results to “Task_1a-out”. For the above small example data set you would report the following  
![image](https://github.com/user-attachments/assets/2bedccb2-8834-4890-9ab1-3975c3e2a5b4)  
## 1b
[Hive] Report the average yearly balance for all people in each education category. Write the results to “Task_1b-out”. For the small example data set you would report the following  
![image](https://github.com/user-attachments/assets/b5921dc7-bb92-4400-b244-7c3183a8806b)  
## 1c
[Spark RDD] Group balance into the following three categories:  
a. Low: -infinity to 500  
b. Medium: 501 to 1500 =>  
c. High: 1501 to +infinity  
Report the number of people in each of the above categories. Write the results to “Task_1c-out” in text file format. For the small example data set you should get the following results (output order is not important in this question):  
![image](https://github.com/user-attachments/assets/fe4de1e8-c5b0-496f-928a-60b58e157b67)  
## 1d
[Spark RDD] Sort all people in ascending order of education. For people with the same education, sort them in descending order by balance. This means that all people with the same education should appear grouped together in the output. For each 
person report the following attribute values: education, balance, job, marital, loan. Write the results to “Task_1d-out” in text file format (multiple parts are allowed). For the small example data set you would report the following
![image](https://github.com/user-attachments/assets/15560622-e218-48f6-8639-e1f5536832de)  

# Task 2: Analysing Twitter Time Series Data
In this task we will be doing some analytics on real Twitter data2. The data is stored in a tab (“\t”) delimited format  
The data is supplied with the assignment at the following locations  
![image](https://github.com/user-attachments/assets/6ec564f9-664e-42f0-b646-1ad57f594192)  
The data has the following attributes  
![image](https://github.com/user-attachments/assets/ce119b80-11c2-410f-b35f-21f684cccd61)  
Here is a small example of the Twitter data that we will use to illustrate the subtasks below:  
![image](https://github.com/user-attachments/assets/d15fb85e-f1ba-4303-8538-1172b9bae878)  
Twitter data source: https://infochimps.com/datasets/twitter-census-conversation-metrics-one-year-of-urls-hashtags-sm
## 2a
Spark RDD] Find the single row that has the highest count and for that row report the month, count and hashtag name. Print the result to the terminal output using println. So, for the above small example data set the result would be:
![image](https://github.com/user-attachments/assets/a7f289f6-62f8-4f92-af75-b1bf5fb86183)  
## 2b
[Do twice, once using Hive and once using Spark RDD] Find the hash tag name that was tweeted the most in the entire data set across all months. Report the total number
of tweets for that hash tag name. You can either print the result to the terminal or
output the result to a text file. So, for the above small example data set the output 
would be:  
abc 1023
## 2c
[Spark RDD] Given two months x and y, where y > x, find the hashtag name that has 
increased the number of tweets the most from month x to month y. Ignore the tweets 
in the months between x and y, so just compare the number of tweets at month x and 
at month y. Report the hashtag name, the number of tweets in months x and y. Ignore 
any hashtag names that had no tweets in either month x or y. You can assume that 
the combination of hashtag and month is unique. Therefore, the same hashtag and 
month combination cannot occur more than once. Print the result to the terminal 
output using println. For the above small example data set:  
![image](https://github.com/user-attachments/assets/9913c06e-6898-42a3-bf87-f827bf4239a1)  

# Task 3: Indexing Bag of Words data
In this task we need to create a partitioned index of words to documents that contain the 
words. Using this index you can search for all the documents that contain a particular word 
efficiently  
The data is supplied with the assignment at the following locations  
![image](https://github.com/user-attachments/assets/d0357ecf-01bb-4c83-8910-7fa24dd22868)  
The first file is called docword.txt, which contains the contents of all the documents stored in the following format:  
![image](https://github.com/user-attachments/assets/2e2c7270-ac64-4de6-87e6-662011a4b7a2)  

The second file called vocab.txt contains each word in the vocabulary, which is indexed by vocabIndex from the docword.txt file.  
Here is a small example content of the docword.txt file  
![image](https://github.com/user-attachments/assets/31641526-7c11-4e1b-aa3e-b2c6ef40a20f)  

Data source: http://archive.ics.uci.edu/ml/datasets/Bag+of+Words
Here is an example of the vocab.txt file  
![image](https://github.com/user-attachments/assets/e0bc7429-fd05-4276-a485-7f5fe1394461)  
## 3a
 [spark SQL] Calculate the total count of each word across all documents. List the 
words in ascending alphabetical order. Write the results to “Task_3a-out” in CSV 
format (multiple output parts are allowed). So, for the above small example input the 
output would be the following:  
![image](https://github.com/user-attachments/assets/f71e9a8b-502c-4148-8c8b-b6eb56f9adc0)  
## 3b
[spark SQL] Create a dataframe containing rows with four fields: (word, docId, count, 
firstLetter). You should add the firstLetter column by using a UDF which extracts the 
first letter of word as a String. Save the results in parquet format partitioned by 
firstLetter to docwordIndexFilename. Use show() to print the first 10 rows of the 
dataframe that you saved.
So, for the above example input, you should see the following output (the exact 
ordering is not important)  
![image](https://github.com/user-attachments/assets/ec493a4e-38df-4aa7-8828-1d4fd26958f5)  
## 3c
[spark SQL] Load the previously created dataframe stored in parquet format from 
subtask b). For each document ID in the docIds list (which is provided as a function 
argument for you), use println to display the following: the document ID, the word 
with the most occurrences in that document (you can break ties arbitrarily), and the 
number of occurrences of that word in the document. Skip any document IDs that 
aren’t found in the dataset. Use an optimisation to prevent loading the parquet file into 
memory multiple times.  
If docIds contains “2” and “3”, then the output for the example dataset would be:  
[2, motorbike, 702]  
[3, boat, 2000]  
For this subtask specify the document ids as arguments to the script. For example:  
$ bash build_and_run.sh 2 3
## 3d
[spark SQL] Load the previously created dataframe stored in parquet format from 
subtask b). For each word in the queryWords list (which is provided as a function 
argument for you), use println to display the docId with the most occurrences of 
that word (you can break ties arbitrarily). Use an optimisation based on how the data 
is partitioned.  
If queryWords contains “car” and “truck”, then the output for the example dataset  
would be:  
![image](https://github.com/user-attachments/assets/7f34d6c5-a296-496a-88c5-06d61fafd511)  
For this subtask you can specify the query words as arguments to the script. For 
example:  
$ bash build_and_run.sh computer environment power  

# Task 4: Creating co-occurring words from Bag of Words data
Using the same data as that for task 3 perform the following subtasks:
## 4a
[spark SQL] Remove all rows which reference infrequently occurring words from 
docwords. Store the resulting dataframe in Parquet format at  
“../frequent_docwords.parquet” and in CSV format at “Task_4a-out”. An infrequently 
occurring word is any word that appears less than 1000 times in the entire corpus of 
documents. For the small example input file the expected output is  
![image](https://github.com/user-attachments/assets/973d52fb-b230-4ee0-affe-49dbf7214057)  
## 4b
[spark SQL] Load up the Parquet file from “../frequent_docwords.parquet” which you 
created in the previous subtask. Find all pairs of frequent words that occur in the same 
document and report the number of documents the pair occurs in. Report the pairs in 
decreasing order of frequency. The solution may take a few minutes to run.  
- Note there should not be any replicated entries like  
o (truck, boat) (truck, boat)  
- Note you should not have the same pair occurring twice in opposite order. Only one of the following should occur:  
o (truck, boat) (boat, truck)  
Save the results in CSV format at “Task_4b-out”. For the example above, the output   
should be as follows (it is OK if the text file output format differs from that below but the  
data contents should be the same):  
boat, motorbike, 2  
motorbike, plane, 2  
boat, plane, 1

For example the following format and content for the text file will also be acceptable  
(note the order is slightly different, that is also OK since we break frequency ties arbitrarily):  
(2,(plane, motorbike))  
(2,(motorbike, boat))  
(1,(plane, boat))  







CREATE DATABASE IF NOT EXISTS QuestionsAnswers;

USE QuestionsAnswers;

CREATE TABLE IF NOT EXISTS Questions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  product_id INT NOT NULL,
  body VARCHAR(1000) NOT NULL,
  date_written VARCHAR(100) NOT NULL,
  asker_name VARCHAR(50) NOT NULL,
  asker_email VARCHAR(100) NOT NULL,
  reported INT NOT NULL,
  helpful INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Answers (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  question_id INT NOT NULL,
  body VARCHAR(1000) NOT NULL,
  date_written VARCHAR(100) NOT NULL,
  answerer_name VARCHAR(50) NOT NULL,
  answerer_email VARCHAR(100) NOT NULL,
  reported INT NOT NULL,
  helpful INT NOT NULL
);

CREATE TABLE IF NOT EXISTS Photos (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  answer_id INT NOT NULL,
  url VARCHAR(250) NOT NULL
);

LOAD DATA LOCAL INFILE '/Users/christopherliang/Desktop/Systems\ Design\ Capstone/starting-point/data-clean/questions-clean.csv'
INTO TABLE Questions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, product_id, body, date_written, asker_name, asker_email, reported, helpful);
-- Query OK, 3521634 rows affected (27.86 sec)
-- Records: 3521634  Deleted: 0  Skipped: 0  Warnings: 0

LOAD DATA LOCAL INFILE '/Users/christopherliang/Desktop/Systems\ Design\ Capstone/starting-point/data-clean/answers-clean.csv'
INTO TABLE Answers
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, question_id, body, date_written, answerer_name, answerer_email, reported, helpful);
-- Query OK, 12392946 rows affected (1 min 21.50 sec)
-- Records: 12392946  Deleted: 0  Skipped: 0  Warnings: 0

LOAD DATA LOCAL INFILE '/Users/christopherliang/Desktop/Systems\ Design\ Capstone/starting-point/data-clean/photos-clean.csv'
INTO TABLE Photos
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(id, answer_id, url);
-- Query OK, 3717892 rows affected (18.88 sec)
-- Records: 3717892  Deleted: 0  Skipped: 0  Warnings: 0

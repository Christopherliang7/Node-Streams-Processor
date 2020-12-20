const fs = require('fs');
const path = require('path');
const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const Transform = require("stream").Transform;

const csvStringifier = createCsvStringifier({
  header: [
    { id: "id", title: "id" },
    { id: "question_id", title: "question_id" },
    { id: "body", title: "body" },
    { id: "date_written", title: "date_written" },
    { id: "answerer_name", title: "answerer_name" },
    { id: "answerer_email", title: "answerer_email" },
    { id: "reported", title: "reported" },
    { id: "helpful", title: "helpful" },
  ],
});

const answersFile = path.join(__dirname, '../data/answers.csv');
const answersResultsFile = path.join(__dirname, '../data-clean/answers-clean.csv')

const myReadAnswersStream = fs.createReadStream(answersFile, 'utf-8')
const myWriteAnswersStream = fs.createWriteStream(answersResultsFile);

class AnswersCleaner extends Transform {
  constructor(options) {
    super(options);
  }
  _transform(chunk, encoding, next) {
    let cleaned = {}
    for (const key in chunk) {
      let trimmed = key.trim();
      cleaned[trimmed] = chunk[key];
    }

    // Remove commas to assure proper data handling during load process
    if (cleaned.body) {
      cleaned.body = cleaned.body.replace(/,/g, '\\,');
    }

    // Checks email to prevent nulls
    if (cleaned.answerer_email === 'null') {
      cleaned.answerer_email = 'Email Not Available'
    }
    
    // Checks Reported for boolean values
    if (cleaned.reported.toLowerCase() === 'true') {
      cleaned.reported = 1;
    } else if (cleaned.reported.toLowerCase() === 'false') {
      cleaned.reported = 0;
    }

    let stringifiedRecord = csvStringifier.stringifyRecords([cleaned]);
    this.push(stringifiedRecord);
    next();
  }
}

const transformer = new AnswersCleaner({ writableObjectMode: true });

myWriteAnswersStream.write(csvStringifier.getHeaderString());

myReadAnswersStream
  .pipe(csv())
  .pipe(transformer)
  .pipe(myWriteAnswersStream)
  .on("finish", () => {
    console.log("Finished writing file!");
  });
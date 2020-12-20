const fs = require('fs');
const path = require('path');
const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const Transform = require("stream").Transform;

const csvStringifier = createCsvStringifier({
  header: [
    { id: "id", title: "id" },
    { id: "product_id", title: "product_id" },
    { id: "body", title: "body" },
    { id: "date_written", title: "date_written" },
    { id: "asker_name", title: "asker_name" },
    { id: "asker_email", title: "asker_email" },
    { id: "reported", title: "reported" },
    { id: "helpful", title: "helpful" },
  ],
});

const questionsFile = path.join(__dirname, '../data/questions.csv');
const questionsResultsFile = path.join(__dirname, '../data-clean/questions-clean.csv')

const myReadQuestionsStream = fs.createReadStream(questionsFile, 'utf-8')
const myWriteQuestionsStream = fs.createWriteStream(questionsResultsFile);

class QuestionsCleaner extends Transform {
  constructor(options) {
    super(options);
  }
  _transform(chunk, encoding, next) {
    let cleaned = {}
    for (let key in chunk) {
      let trimmed = key.trim();
      cleaned[trimmed] = chunk[key];
    }

    // Remove commas to assure proper data handling during load process
    if (cleaned.body) {
      cleaned.body = cleaned.body.replace(/,/g, '\\,');
    }

    // Checks to make sure true and false = 0 and 1
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

const transformer = new QuestionsCleaner({ writableObjectMode: true });

myWriteQuestionsStream.write(csvStringifier.getHeaderString());

myReadQuestionsStream
  .pipe(csv())
  .pipe(transformer)
  .pipe(myWriteQuestionsStream)
  .on("finish", () => {
    console.log("Finished writing file!");
  });
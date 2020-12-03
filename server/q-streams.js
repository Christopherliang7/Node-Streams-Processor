const fs = require('fs');
const path = require('path');
const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const Transform = require("stream").Transform;

const csvStringifier = createCsvStringifier({
  // Headers
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

// File Connections
const questionsFile = path.join(__dirname, '../data/questions.csv');
const questionsResultsFile = path.join(__dirname, '../data-clean/questions-clean.csv')

// Streams
const myReadQuestionsStream = fs.createReadStream(questionsFile, 'utf-8')
const myWriteQuestionsStream = fs.createWriteStream(questionsResultsFile);

// Data Cleaner Function
class CSVCleaner extends Transform {
  constructor(options) {
    super(options);
  }
  _transform(chunk, encoding, next) {
    let cleaned = {}
    for (const key in chunk) {
      let trimmed = key.trim();
      cleaned[trimmed] = chunk[key];
    }

    if (cleaned.reported.toLowerCase() === 'true') {
      cleaned.reported = 1;
    } else if (cleaned.reported.toLowerCase() === 'false') {
      cleaned.reported = 0;
    }

    // use csvStringifier to turn our chunk into a csv string
    let stringifiedRecord = csvStringifier.stringifyRecords([cleaned]);
    this.push(stringifiedRecord);
    next();
  }
}

// Transform function
const transformer = new CSVCleaner({ writableObjectMode: true });

// Write Referencing Headers
myWriteQuestionsStream.write(csvStringifier.getHeaderString());

// Initialization
myReadQuestionsStream
  .pipe(csv())
  .pipe(transformer)
  .pipe(myWriteQuestionsStream)
  .on("finish", () => {
    console.log("Finished writing file!");
  });
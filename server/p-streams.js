const fs = require('fs');
const path = require('path');
const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const Transform = require("stream").Transform;

const csvStringifier = createCsvStringifier({
  // Headers
  header: [
    { id: "id", title: "id" },
    { id: "answer_id", title: "answer_id" },
    { id: "url", title: "url" },
  ],
});

// File Connections
const photosFile = path.join(__dirname, '../data/answers_photos.csv');
const photosResultsFile = path.join(__dirname, '../data-clean/photos-clean.csv')

// Streams
const myReadPhotosStream = fs.createReadStream(photosFile, 'utf-8')
const myWritePhotosStream = fs.createWriteStream(photosResultsFile);

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

    // photo checker
    if (cleaned.url.length === 0) {
      cleaned.url = "https://images.unsplash.com/photo-1530519729491-aea5b51d1ee1?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=1651&q=80"
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
myWritePhotosStream.write(csvStringifier.getHeaderString());

// Initialization
myReadPhotosStream
  .pipe(csv())
  .pipe(transformer)
  .pipe(myWritePhotosStream)
  .on("finish", () => {
    console.log("Finished writing file!");
  });
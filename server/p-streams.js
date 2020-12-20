const fs = require('fs');
const path = require('path');
const csv = require("csv-parser");
const createCsvStringifier = require("csv-writer").createObjectCsvStringifier;
const Transform = require("stream").Transform;

const csvStringifier = createCsvStringifier({
  header: [
    { id: "id", title: "id" },
    { id: "answer_id", title: "answer_id" },
    { id: "url", title: "url" },
  ],
});

const photosFile = path.join(__dirname, '../data/answers_photos.csv');
const photosResultsFile = path.join(__dirname, '../data-clean/photos-clean.csv')

const myReadPhotosStream = fs.createReadStream(photosFile, 'utf-8')
const myWritePhotosStream = fs.createWriteStream(photosResultsFile);

class PhotosCleaner extends Transform {
  constructor(options) {
    super(options);
  }
  _transform(chunk, encoding, next) {
    let cleaned = {}
    for (const key in chunk) {
      let trimmed = key.trim();
      cleaned[trimmed] = chunk[key];
    }

    let stringifiedRecord = csvStringifier.stringifyRecords([cleaned]);
    this.push(stringifiedRecord);
    next();
  }
}

const transformer = new PhotosCleaner({ writableObjectMode: true });

myWritePhotosStream.write(csvStringifier.getHeaderString());

myReadPhotosStream
  .pipe(csv())
  .pipe(transformer)
  .pipe(myWritePhotosStream)
  .on("finish", () => {
    console.log("Finished writing file!");
  });
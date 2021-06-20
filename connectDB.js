require('dotenv').config();
const MongoClient = require('mongodb').MongoClient;
const url = process.env.MONGO_URI
const dbName = 'testimony';
const json_first = require('./first.json');
const json_second = require('./second.json');

const client = new MongoClient(url, { useUnifiedTopology: true })

const start = async () => {
  try {
    await client.connect();
    console.log('Mongo connected');
    const firstCollection = client.db(dbName).collection('firstCollection');
    const secondCollection = client.db(dbName).collection('secondCollection');

    // Insert first json to first collection
    var firstJSON_parsed = JSON.parse(JSON.stringify(json_first))
    await firstCollection.insertMany(firstJSON_parsed);

    // Insert second json to second collection
    var secondJSON_parsed = JSON.parse(JSON.stringify(json_second))
    await secondCollection.insertMany(secondJSON_parsed);

    let newColl = firstCollection.aggregate([
      // Extract ll and put as separate fields
      {
        $set: {
          longitude: { $arrayElemAt: [ '$location.ll', 0] }, 
          latitude: { $arrayElemAt: [ '$location.ll', 1] },
        }
     },
     // To find difference in the next step
     {
        $lookup:
        {
          from: "secondCollection",
          localField: "country",
          foreignField: "country",
          as: "secondCollection"
        }
      },
      // As Nina said I should follow strictly step by step, that's why I put set second time
      {
        $set: {
          difs: { $subtract: [ {$sum: "$students.number"} ,{ $arrayElemAt: [ '$secondCollection.overallStudents', 0] }] }
          
        }
      },
      {
         $project: { secondCollection: 0} 
      },
      // Write result in new collection
      {
        $out: { db: dbName, coll: "thirdCollection" } 
      }
        
    ]);
    newColl.next(); 
  } 
  catch (error) {
    console.log(error);
  }
}
start();
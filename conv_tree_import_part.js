const fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
const {
    Worker, isMainThread, parentPort, workerData
} = require('worker_threads');
var local_path = "C:\\Users\\Bocko\\Downloads\\data\\edata" + "\\"
const readline = require('readline');
var franc = require('franc-min')
var count = 0

//OLD https://github.com/dominictarr/JSONStream //You can specify which things you want to parse to make shit zoom
async function checkFile(local_p, input_file, parentPort, workerID, dbo) {
    console.time(input_file)
    var inputStream = fs.createReadStream(local_p + input_file, { encoding: 'utf-8' });
    const rl = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity
    });
    var check = []
    var ids = []
    //get user mentions id and just add it to the db thingie
    rl.on('line', (input) => {
        try {
            let one_line = JSON.parse(input)
            if(one_line.in_reply_to_user_id == null){
                checkWho(currItem.entities.user_mentions, ids)
                .then(x => {
                    db.collection("mentions").insertOne({id: currItem.id, a_id: x}).catch(() => {})
                })
            }
            /*
            dbo.collection("delet")
            .countDocuments({id: one_line.id})
            .then(array_count => {
                if(array_count == 0){
                    dbo.collection('edata').insertOne(one_line).catch((err) => {
                        console.log(err)
                    })
                }
            })
            */
        } catch (error) {
            //sendMessage({ type: "bad_input", data: input })
        }
        //WRITE TO OUTPUT PIPE
    });
    rl.on('close', () => {
        console.timeEnd(input_file)
    })
    function getAirline(text, airlines, ids){
        text.split(" ").forEach(x => {
            if(airlines.includes(text)){
                resolve
            }
        })
    }
    function sendMessage(message) {
        let messageBody = {
            workerID: workerID,
            workPacket: input_file,
            message: message
        }
        parentPort.postMessage(messageBody)
    }
}


if (isMainThread) {
    allResults = [];
    var max_workers = 5;
    var worker_list = Array(max_workers).fill(0)
    var output = fs.createWriteStream(local_path + "output.json", { encoding: 'utf-8' })
    fs.readdir(local_path, function (err, located_files) {
        err ? console.log('Unable to scan directory: ' + err) : null


        ///
        const express = require('express')
        const app = express()
        const port = 3000

        app.get('/', (req, res) => {
            //output.write(JSON.stringify(packets))
            //res.sendStatus(200)
            res.json(count)
        })

        app.listen(port, () => console.log(`Trigger http://localhost:${port}`))

        ///


        located_files = located_files.filter(x => x.includes("airlines-"))
        //located_files = located_files.slice(0, 1)
        console.log("Started task with", max_workers, "workers processing", located_files.length, "files")
        for (i = 0; i < located_files.length; i++) {
            worker_list[i % max_workers] += 1
        }
        console.log("Work distribution", worker_list)
        if (worker_list.reduce((x, y) => x + y) === located_files.length) {
            worker_list.forEach(async (item, index) => {
                var files = located_files.splice(0, item)
                const worker = new Worker(__filename, {
                    workerData: { files: files, workerID: index }
                });
                worker.on('message', (message) => {
                    if (typeof (message.message) === typeof ("")) {
                        console.log("Message from ", message.workerID, ":", message.message)
                    }
                    if (typeof (message.message) === typeof ([])) {
                        if (message.message.type == "bad_input") {
                            output.write(message.message.data + " //" + message.workPacket + "\n")
                        }
                    }
                });
                worker.on('error', (err, workerData) => {
                    console.log("Error", err, files)
                    worker.terminate()
                });
                worker.on('exit', () => {
                    max_workers--;
                    if (max_workers == 0) {
                        console.log("Log written to output.json")
                        output.write(JSON.stringify(allResults))
                    }
                })

            })
        } else {
            console.log("The math doesn't work out, please specify another amount of workers...")
        }
    })
} else {
    const client = new MongoClient('mongodb://localhost:27017/', { useNewUrlParser: true, useUnifiedTopology: true });
    client.connect().then(() => {
        const db = client.db('edata')
        //workerData.files.forEach(async x => checkFile(local_path, x, parentPort, workerData.workerID, db))
        workerData.files.forEach(async x => checkFile(local_path, x, parentPort, workerData.workerID, db))

    })
}



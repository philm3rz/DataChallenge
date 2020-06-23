const fs = require('fs');

const {
    Worker, isMainThread, parentPort, workerData
} = require('worker_threads');
var local_path = "C:\\Users\\Bocko\\Downloads\\data\\edata\\testing_area" + "\\"
const readline = require('readline');
var franc = require('franc-min')
var MongoClient = require('mongodb').MongoClient;
async function checkFile(local_p, input, output, parentPort, workerID, db, ids) {
    console.time(input)
    var inputStream = fs.createReadStream(local_p + input, { encoding: 'utf-8' });
    //var outputStream = fs.createWriteStream(local_p + output, { encoding: 'utf-8' })
    const rl = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity
    });
    rl.on('line', (input) => {
        try {
            //console.log(input)
            db.collection('mentions').insertOne(JSON.parse(input))
        } catch (error) {
            //sendMessage({ type: "bad_input", data: input })
        }
        //WRITE TO OUTPUT PIPE
    });
    rl.on('close', () => {
        console.timeEnd(input)
    })
}


if (isMainThread) {
    allResults = [];
    var max_workers = 1;
    var worker_list = Array(max_workers).fill(0)
    var output = fs.createWriteStream(local_path + "output.json", { encoding: 'utf-8' })
    fs.readdir(local_path, function (err, located_files) {
        err ? console.log('Unable to scan directory: ' + err) : null
        located_files = located_files.filter(x => x.includes("whose") && x.includes('json'))
        console.log(located_files)
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
                            //output.write(message.message.data + " //" + message.workPacket + "\n")
                        }
                        if (message.message.type == "whose") {
                            output.write(JSON.stringify(message.message.data) + "\n")
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
        x = workerData.files[0]
        console.log(x)
        checkFile(local_path, x, '\\edata\\' + x, parentPort, workerData.workerID, db, [])
    })
}



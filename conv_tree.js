const fs = require('fs');
var MongoClient = require('mongodb').MongoClient;
require('mongodb')
const {
    Worker, isMainThread, parentPort, workerData
} = require('worker_threads');
var local_path = "C:\\Users\\Bocko\\Downloads\\data\\edata" + "\\"
const readline = require('readline');

//OLD https://github.com/dominictarr/JSONStream //You can specify which things you want to parse to make shit zoom
async function checkFile(local_p, file_name, parentPort, workerID, db) {
    //console.time(input)
    var inputStream = fs.createReadStream(local_p + file_name, { encoding: 'utf-8' });
    const rl = readline.createInterface({
        input: inputStream,
        crlfDelay: Infinity
    });
    var unique = [];
    /*
    if(unique.includes(json_parsed.id)){
                    console.log(file_name, json_parsed.id)
                }
                unique.push(json_parsed.id)
    */
    rl.on('line', async (input) => {
        try {
            var json_parsed = JSON.parse(input)
            //https://docs.mongodb.com/manual/reference/operator/query/elemMatch/
            if (json_parsed.in_reply_to_status_id == null) {
                prune(json_parsed.id, [], json_parsed.id)
            }
        } catch (error) {
            console.log(error)
            //sendMessage({ type: "bad_input", data: input })
        }
        //WRITE TO OUTPUT PIPE
    });
    //keep track of responses and just replace the response array items with more response objects
    ////KEEP TRACK OF INITIATOR ID SO WE KNOW THE ENTIRE GROUP
    function prune(node_id, previously_seen, root_node) {
        //The problem is a race condition between putting shit in and sending it, it should first make sure it sent the message
        //Just assemble shit here - create a class that supports item adding by reference and searches through object for it
        //  Or have a storage with a simplified array representing the ids in the object structure to speed up search
        if (!previously_seen.includes(node_id)) {
            previously_seen.push(node_id)
            db.countDocuments({ $and: [{ id: { $gt: node_id }, in_reply_to_status_id: node_id }] }).then(reply_count => {
                if (reply_count != 0) {
                    db.find({ $and: [{ id: { $gt: node_id }, in_reply_to_status_id: node_id }] })
                        .toArray()
                        .then(result_array => {
                            if (result_array.length != 0) { //TODO-REMOVE
                                sendMessage({type: 1, initiator: parseInt(root_node), res_to: node_id, responses: result_array.map(x => x.id)})
                                .then(() => {
                                    result_array.forEach(x=> {
                                        prune(x.id, previously_seen, root_node)
                                    })
                                })
                            }
                        })
                } else {
                    //Handle items with no replies here
                }
            })
        }
    }
    //prune(1135831868836700200, [], 1135831868836700200)
    //prune(1135831868836700200, [], 1135831868836700200)
    rl.on('close', () => {
        //console.timeEnd(input)
    })
    function sendMessage(message) {
        return new Promise((resolve, reject) => {
            let messageBody = {
                workerID: workerID,
                workPacket: file_name,
                message: message
            }
            parentPort.postMessage(messageBody)
            resolve()
        })
    }
}


if (isMainThread) {
    allResults = [];
    var max_workers = 15;
    var output = fs.createWriteStream(local_path + "output.json", { encoding: 'utf-8' })
    const client = new MongoClient('mongodb://localhost:27017/', { useNewUrlParser: true, useUnifiedTopology: true });
    var missing_item_registry = []
    client.connect().then(() => {
        const db = client.db('edata').collection('conv_tree')
        fs.readdir(local_path, function (err, located_files) {

            ///
            const express = require('express')
            const app = express()
            const port = 3000

            app.get('/', (req, res) => {
                output.write(JSON.stringify(packets))
                //res.sendStatus(200)
                res.sendStatus(200)
            })

            app.listen(port, () => console.log(`Trigger http://localhost:${port}`))

            ///
            err ? console.log('Unable to scan directory: ' + err) : null
            packets = {}
            var last_conversation = 0;
            sample_interval = 5000;
            setInterval(function () {
                let unique_conversations = Object.keys(packets).length
                let conversations_ps = (unique_conversations - last_conversation) / (sample_interval / 1000)
                last_conversation = unique_conversations
                if (conversations_ps == 0) {
                    console.log('Idle...')
                } else {
                    console.log("Conversations: ", unique_conversations)
                    console.log("Conversations per second", conversations_ps)
                    //console.log(JSON.stringify(packets))
                }
            }, sample_interval);
            located_files = located_files.filter(x => x.includes("airlines-"))
            //located_files = located_files.slice(0, 100)
            if (max_workers > located_files.length) {
                max_workers = located_files.length
            }
            var worker_list = Array(max_workers).fill(0)
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
                        if (message.message.type == 1) {
                            msg = message.message
                            if (msg.res_to == msg.res_to) {//msg.initiator
                                if (!packets[msg.initiator]) {
                                    packets[msg.initiator] = { convs: [] }
                                }
                                packets[msg.initiator].convs.push([
                                    msg.res_to,
                                    msg.responses
                                ])
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
                            //console.log("Log written to output.json")
                            //output.write(JSON.stringify(allResults))
                            console.log("All workers exited")
                        }
                    })

                })
            } else {
                console.log("The math doesn't work out, please specify another amount of workers...")
            }
        })
    })
} else {
    const client = new MongoClient('mongodb://localhost:27017/', { useNewUrlParser: true, useUnifiedTopology: true });
    client.connect().then(() => {
        const db = client.db('edata').collection('edata')
        workerData.files.forEach(async x => checkFile(local_path, x, parentPort, workerData.workerID, db))
    })
}



const fs = require('fs');
//DOCS FOR API https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data
const {
    Worker, isMainThread, parentPort, workerData
} = require('worker_threads');
var local_path = "C:\\Users\\Bocko\\Downloads\\data" + "\\"
var generated_path = local_path + "generated\\"

var franc = require('franc-min')
var es = require('event-stream')
const Database = require('better-sqlite3');
//https://github.com/mapbox/node-sqlite3

/*
The python module should be able to do IPC (or whatever is fastest)
Use Numba to speed up Python part
https://www.zerorpc.io/

Get interesting users and per-user stats
    -Lots of retweet bots like 47501959 (airlines prob don't want that - could be adversarial tho (do sentiment analysis on bot tweets))
        -Accounts with lots of statuses but low followers (and those followers prob are bots)
        -Could be interesting to detect bot activity
        THE AMOUNT IS ASTONISHING

Create update history through metadata - save updates in formats like
    {'ID': [
        {'timestamp_ms': 10000000, 'state': likes, retweets, responses}
    ]}

User growth doesn't really matter
    - Just keep the newest data through the same update system

Create tree
    - https://typeofnan.dev/an-easy-way-to-build-a-tree-with-object-references/

Track customer agility (how they move across companies)
    - Track customer location if available (multiple branches multiple people multiple responses, can more easily classify problems as either 
        corporate or single-branch-based)


https://github.com/voodooattack/shared-buffer

Sentiment connected to certain words - airlines can look up how people feel about a certain word (or how is it used)
Intent analysis
Sentiment analysis https://monkeylearn.com/sentiment-analysis/
Automatic sorting/verification of sentiment analysis using autoencoders to verify that something looks like that (multiple sources of information to boost quality)
https://towardsdatascience.com/lstm-autoencoder-for-extreme-rare-event-classification-in-keras-ce209a224cfb
USING THE MODEL TO PREDICT GROWTH WE COULD MARK ANOMALIES (STD HIGHER THAN X ASSUMING MODEL IS PERFORMING FINE -> ALERT)
https://en.wikipedia.org/wiki/Word2vec
https://github.com/loretoparisi/word2vec-twitter
https://fredericgodin.com/research/twitter-word-embeddings/
https://cloud.google.com/dialogflow/docs/how/sentiment
//https://github.com/anuragmishra1/alter-nlu
        -Use sentiment extraction to find most common words in dataset and create rule-based actions supported by vec2word to add flexibility
        --Use case scenario (Lets say we find tweets that ask when is flight x landing - if the confidence level is above x we then automatically respond with the data)
            --Doesn't even have to be automatic responses - can be integrated into the tooling used by the response team to speed up responses
                --(Popup (HEY IS THIS THE RIGHT RESPONSE) OR SOMETHING THAT INTEGRATES INTO THEIR TOOLING)
        --Use case scenario (For static analysis - we can just look at what is the most often mentioned thing - often mentioned complaint long term and see how twitter
            sentiment changes after implementation (track per word/phrase sentiment developments over time))
                --Use case scenario (If an airline has a problem with bathrooms being dirty - and if people complain - we should be able to see that the word toilet
                    is mentioned negatively, and the whole thing is easily configurable)
                        --Use case scenario (We can also compare against other airlines to look for quick and cheap brand improvements like throwing shade at the company
                            through a twitter post that they have good toilets/food/whatever (educated material investment powered by twitter sentiment))


https://snips-nlu.readthedocs.io/en/latest/dataset.html
https://github.com/SUTDBrainLab/intent-capsnet

Interesting tweets:
https://twitter.com/Ryanair/status/667708537775202304
    -ppl don't know how to use fucking twitter

Interesting accounts:
{"id":799303160452018200,"name":"British Airways SUCK","verified":false,"followers_count":1565,"friends_count":3043,"listed_count":1,"favourites_count":759,"statuses_count":2518,"location":null}
*/


//OLD https://github.com/dominictarr/JSONStream //You can specify which things you want to parse to make shit zoom
//--trace-opt --future
///https://www.npmjs.com/package/sqlite-to-mongo
//https://www.sqlite.org/json1.html

function makeid(length) {
    var result           = '';
    var characters       = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var charactersLength = characters.length;
    for ( var i = 0; i < length; i++ ) {
       result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
 }

function checkFile(search_directory, input_file, output_file, parentPort, workerID, x, airline_ids, blacklist, schemas, outputs, cust_id) {
    return new Promise((resolve, reject) => {
        //var outputStream = fs.createWriteStream(local_p + output, { encoding: 'utf-8' })
        //HIJACK ERROR FUNCTION BECAUSE USING TRY CATCH DISABLES OPTIMIZATIONS IN V8
        console.error = (error, static_text, parse_attempt) => {
            if (static_text !== undefined) { //Not really interested in parsing errors that set this value to something
                outputs.trash.write(parse_attempt + "\n")
            } else {
                console.log(error)
            }
        }
        //FIRST PASS OUTPUT ALL IDS, SORT THEM AND THEN CHECK FOR DUPLICATES - CHECK IF ALSO DUPLICATE TEXT AND IF NOT CHANGE IDS AROUND TO WORK

        //PER-TWEET-TYPE
        //IF RETWEET JUST STORE THE ID OF WHAT IS RETWEETED
        //WHEN QUOTING A STATUS IT SHOULD GRAB THE UPDATED ANALYTICS OF A TWEET -------URGENT
        //setInterval(() => {console.log(update_count)}, 3000)
        fs.createReadStream(search_directory + input_file, { encoding: 'utf-8' })
            .pipe(es.split()) //defaults to lines.
            .pipe(es.parse())
            .pipe(es.map((line, cb) => {
                if (line.created_at !== undefined) {
                    //PROD outputs.ids.write(line.id + "," + line.timestamp_ms + "\n")
                    removeKeys(line, outputs, schemas)
                    //.then(x => console.log(Object.keys(x.object)))
                    .then(x => file_write(x.object, x.outputs.tweets, x.schemas.tweets))

                } else {
                    //CAN HANDLE DELETED AND OTHER THINGS HERE BY WRITING TO OTHER STREAMS
                    outputs.to_delete.write(line.delete.status.id + ',' + line.delete.timestamp_ms + "\n")
                    cb()
                }
            }))
            .on('end', () => {
                resolve(input_file)
            }) //DUMP TO MONGO
            .on('error', function (err) {
                console.log('Error while reading file.', err);
            })

        function removeKeys(object, outputs, schemas, remove_text = false) {
            return new Promise((r, re) => {
                object.user = {
                    id: object.user.id,
                    name: object.user.name,
                    verified: object.user.verified,
                    followers_count: object.user.followers_count,
                    friends_count: object.user.friends_count,
                    listed_count: object.user.listed_count,
                    favourites_count: object.user.favourites_count,
                    statuses_count: object.user.statuses_count,
                    location: object.user.location,
                    statuses_count: object.user.statuses_count
                } //save this to a db
                if (object.timestamp_ms != undefined) {
                    user_update(object.user, object.timestamp_ms)
                }
                object.user = object.user.id
                if (remove_text == false) {
                    object.text = object.truncated ? object.extended_tweet.full_text : object.text
                    object.text = object.retweeted_status ? null : object.text //CLEAR TEXT IF RETWEET BCZ SAVING //UNDER ASSUMPTION OF TWEET EXISTING SOMEWHERE ELSE
                    if(object.text != null && !object.retweeted_status & !object.quoted_status){
                        object.language = franc(object.text)
                    }
                } else {
                    delete object.text
                }

                if (object.retweeted_status) {
                    removeKeys(object.retweeted_status, outputs, schemas, true).then(
                        x => file_write(x.object, x.outputs.updates_rt, x.schemas.retweets)
                    )
                    object.retweeted_status = object.retweeted_status.id
                }
                if (object.quoted_status) {
                    removeKeys(object.quoted_status, outputs, schemas, true).then(
                        x => file_write(x.object, x.outputs.updates_qt, x.schemas.quotes)
                    )
                    object.quoted_status = object.quoted_status.id
                }
                //THE HANDLING LOGIC CAN EITHER CREATE A LOG OF EVERY UPDATE WITH A NESTED ARRAY
                //OR IT CAN JUST REPLACE THE LAST INFO WITH THE NEWEST
                ///---PROB SHOULD UPDATE THE TWEET WITH LATEST DATA BUT ALSO KEEP HISTORICAL RECORDS
                if (object.is_quote_status) {
                    delete object.is_quote_status
                }
                if(object.entities){
                    write_entities(object.entities, object.id, outputs)
                    //entities.write(object.id + "," + new Buffer(JSON.stringify(object.entities)).toString('base64') + "\n")
                    delete object.entities
                }
                blacklist.forEach((item) => {
                    delete object[item]
                })
                r({object: object, outputs: outputs, schemas: schemas})
            })
        }
        function user_update(user_obj, timestamp) {
            return new Promise((r) => {
                //ADD SCHEMA
                //outputs.user_updates.write(user_obj.id + ',' + timestamp + ',' + Buffer.from(JSON.stringify(user_obj)).toString('base64') + "\n")
                file_write(user_obj, outputs.user_updates, schemas.users, timestamp)
                r()
            })
        }

        function checkWho(mentions, owner) {
            return new Promise((r, re) => {
                mentions.forEach((x) => {
                    if (airline_ids.includes(x.id)) {
                        r(x.id)
                    }
                })
                re(owner)
            })
        }
        function sendMessage(message, type = 'normal') {
            return new Promise((r) => {
                let uint8Array = Buffer.from(JSON.stringify({
                    workerID: workerID,
                    workPacket: input_file,
                    message: message,
                    type: type
                }));
                parentPort.postMessage(uint8Array, [uint8Array.buffer])
                r()
            })
        }

        function file_write(line, where, schema, optional_id = null) {
            
            //CHECK IF TYPE OBJ THEN B64
            //console.log(optional_id + ">>" +)
            //console.log(where == null)
            if(optional_id == null){
                where.write(schema.map(x => encodeURIComponent(check_b64_required(line[x]))).join(',') + "\n")
            }else{
                where.write(optional_id + "," + schema.map(x => encodeURIComponent(check_b64_required(line[x]))).join(',') + "\n")
            }
            
            //console.log(schema.map(x=>check_b64_required(line[x]))
        }
        function check_b64_required(x){
            if(typeof(x) == 'object' && x != null){
                //console.log()
                return Buffer.from(JSON.stringify(x)).toString('base64')
            }
            return x
        }
        function write_entities(line, id, outputs){
            const hashtag_schema = [
                'text',
            ]
            line.hashtags.forEach(x=>{
                file_write(x, outputs.ent_hashtag, hashtag_schema, id)
            })
            const url_schema = [
                'expanded_url',
            ]
            line.urls.forEach(x=>{
                file_write(x, outputs.ent_urls, url_schema, id)     
            })
            const user_mentions_schema = [
                'name',
                'id',
            ]
            line.user_mentions.forEach(x=>{
                file_write(x, outputs.ent_mentions, user_mentions_schema, id)
            })
        }
    })
}

if (isMainThread) {
    //400K DUPLICATE FUCKING IDS - 414838 exactly
    let allResults = [];
    var max_workers = 10;
    var worker_list = Array(max_workers).fill(0)
    const output = fs.createWriteStream(generated_path + "output.json", { encoding: 'utf-8' })
    fs.readdir(generated_path, function (err, located_files) {
        err ? console.log('Unable to scan directory: ' + err) : null
        located_files.map(x => {
            fs.unlink(generated_path + x, () => { })
        })
    })
    fs.readdir(local_path, function (err, located_files) {
        err ? console.log('Unable to scan directory: ' + err) : null
        located_files = located_files.filter(x => x.includes(".json"))
        //located_files = located_files.slice(0, 1)
        console.log("Started task with", max_workers, "workers processing", located_files.length, "files")
        for (i = 0; i < located_files.length; i++) {
            worker_list[i % max_workers] += 1
        }
        console.log("Work distribution", worker_list)
        //COUNT INSTANCES OF NULL VALUES TO FIGURE OUT WHAT IS LOW VARIANCE
        //MOVE USER FIELD TO ANOTHER DB - DEDUPLICATING USER DATA (Especially valuable for airline user items)
        //TO REPAIR DUPLICATE IDS
        //-FIND FIRST DUPLICATE - AND STORE THE VALUE, EVERYTHING THAT POINTS TO VALUES ABOVE THAT IS ASSUMED DUPLICATE AND ADD AN OFFSET TO IT UNTIL THE COLLISIONS REMOVE
        //-WHEN TRYING TO FIND ITEMS BELOW ID - DO IT WITHOUT OFFSET
        /*
        console.log(stmt.all())
        */
        if (worker_list.reduce((x, y) => x + y) === located_files.length) {
            worker_list.forEach((item, index) => {
                //Memory leak prob caused by this closure - should remove this and just check if arrays still emit events        
                const files = located_files.splice(0, item)
                const worker = new Worker(__filename, {
                    workerData: { files: files, workerID: index }
                });
                worker.on('message', (message) => {
                    message = JSON.parse(new TextDecoder("utf-8").decode(message))
                    let message_body = message.message
                    let message_type = message.type
                });
                worker.on('error', (err, workerData) => {
                    console.log("Error", err, files)
                    worker.terminate()
                });
                worker.on('exit', () => {
                    max_workers--;
                    //console.log("Worker exiting, remaining", max_workers)
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
    const cust_id = makeid(32)
    const db = []
    const airline_ids = [56377143, 106062176, 18332190, 22536055, 124476322, 26223583, 2182373406, 38676903, 1542862735, 253340062, 218730857, 45621423, 20626359]
    const blacklist = ["utc_offset", "time_zone", "lang", "geo_enabled", "following", "follow_request_sent", "has_extended_profile", "notifications", "profile_location", "contributors_enabled", "profile_image_url", "profile_background_color", "profile_background_image_url", "profile_background_image_url_https", "profile_background_tile", "profile_link_color", "profile_sidebar_border_color", "profile_sidebar_fill_color", "profile_text_color", "profile_use_background_image", "is_translator", "is_translation_enabled", "translator_type",
        , "source", "display_text_range", "resize", "url", "w", "h", "in_reply_to_screen_name", "in_reply_to_status_id_str", "in_reply_to_user_id_str", "extended_entities", "geo", "filter_level",
        "display_text_range", "truncated", "coordinates", "place", "extended_tweet", "created_at", "name", "screen_name", "url", "description", "protected", "listed_count", "favourites_count", "statuses_count", "created_at",
        "profile_banner_url", "profile_image_url_https", "default_profile", "withheld_in_countries", "default_profile_image", "favorited", "retweeted", "possibly_sensitive", "quoted_status_id", "quoted_status_id_str", "quoted_status_permalink", "id_str", "contributors",
        "reply_count", "quote_count", "retweet_count", "favorite_count"]
    var counter = workerData.files.length //AMOUNT OF FILES LEFT
    const options = { flags: 'a' }
    const ids = fs.createWriteStream(generated_path + "ids.csv", options)
    //ids.write('id,timestamp\n')
    const to_delete = fs.createWriteStream(generated_path + "to_delete.csv", options)
    //to_delete.write('tid,timestamp\n')
    const trash = fs.createWriteStream(generated_path + "trash.csv", options)
    //trash.write('input\n')
    const updates_rt = fs.createWriteStream(generated_path + "updates_rt.csv", options)
    //updates_rt.write('')
    const updates_qt = fs.createWriteStream(generated_path + "updates_qt.csv", options)
    //updates_qt.write('')
    const user_updates = fs.createWriteStream(generated_path + "user_updates.csv", options)
    //user_updates.write('')
    const tweets = fs.createWriteStream(generated_path + "tweets.csv", options)
    //user_updates.write('')
    const ent_hashtag = fs.createWriteStream(generated_path + "ent_hashtag.csv", options)
    //entities.write('')
    const ent_mentions = fs.createWriteStream(generated_path + "ent_mentions.csv", options)
    //entities.write('')
    const ent_urls = fs.createWriteStream(generated_path + "ent_urls.csv", options)
    //entities.write('')
    const tweet_schema = [
        'id',
        'text',
        'in_reply_to_status_id',
        'in_reply_to_user_id',
        'user',
        'retweeted_status',
        'quoted_status',
        'language',
        'timestamp_ms',
    ]
    const users_schema = [
        'id',
        'name',
        'verified',
        'followers_count',
        'friends_count',
        'listed_count',
        'favourites_count',
        'statuses_count',
        'location'
      ]
    const retweet_schema = [
        'id',
        'in_reply_to_status_id',
        'in_reply_to_user_id',
        'user',
        //'is_quote_status',
        //'entities',
        //'quoted_status',
        //'scopes'
      ]
    const quoted_schema = [
        'id',
        'in_reply_to_status_id',
        'in_reply_to_user_id',
        'user',
        //'is_quote_status',
        //'entities',
        //'scopes'
      ]
    const schemas = {
        tweets: tweet_schema,
        users: users_schema,
        retweets: retweet_schema,
        quotes: quoted_schema
    }
    const outputs = {
        ids: ids,
        to_delete: to_delete,
        trash: trash,
        updates_rt: updates_rt,
        updates_qt: updates_qt,
        user_updates: user_updates,
        tweets: tweets,
        ent_hashtag: ent_hashtag,
        ent_mentions: ent_mentions,
        ent_urls: ent_urls
    }
    workerData.files.forEach(x => {
        checkFile(local_path, x, '\\edata\\' + x, parentPort, workerData.workerID, db, airline_ids, blacklist, schemas, outputs, cust_id)
            .then(x => {counter--})
            .catch(x => {console.log})
    })
}
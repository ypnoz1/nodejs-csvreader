const fs = require('fs');
const Papa = require('papaparse');  
const {getDistanceBetweenPoints, isValidTimeStamp} = require('../utils/tools');  
const { TIME_STAMP_TO_DAYS } = require('../utils/constant');  

const getMainFile = ((req, res) => {
    const start = parseInt(req.query.start) ?? 0;
    let end     = parseInt(req.query.end) ?? 250;
    
    const dataByMonthFolder  = 'src/files/saved_data/months/';
    const csvFile  = `${dataByMonthFolder}data_month_1_1.csv`;
    let readStream = fs.createReadStream(csvFile/*, { highWaterMark: 8 * 1024 }*/);
    
    end = start === 0 ? end + 1 : end;
    let countLine  = 0;
    let saveResult = [];  
    Papa.parse(readStream, {    
        delimiter: ",",
        newline: "\n",
        header: false,  
        dynamicTyping: true,   
        skipEmptyLines: true,
        chunk: function (result, parser) {
            parser.pause();
            result.data.forEach(async elem => {    
                             
                if(countLine >= start && countLine < end){ 
                    saveResult.push({
                        id:  Object.hasOwn(elem, 0) ? elem[0] : '',
                        lat: Object.hasOwn(elem, 1) ? elem[1] : '',
                        lon: Object.hasOwn(elem, 2) ? elem[2] : '',
                        delta_time: Object.hasOwn(elem, 3) ? elem[3] : '',
                        timestamp: Object.hasOwn(elem, 4) ? elem[4] : '',
                    });
                }
                if(countLine === end){
                    res.end(JSON.stringify(saveResult));
                    saveResult = [];  
                    countLine++;                   
                    parser.abort(); 
                    return;
                }
                countLine++;
            });                
        },
        complete: function() {      
            console.log("All rows successfully processed");   
            res.end();  
        },
        error: function(error) {     
            console.error("Parsing error:", error.message);    
        }
    });
});
const getFileByMonth = ((req, res) => {
    let entries     = req.query.e ? parseInt(req.query.e) : 0;
    let allChunk    = req.query.c ? parseInt(req.query.c) : 0;
    const month     = req.query.m ? parseInt(req.query.m) : 1;
    const maxPart   = req.query.p ? parseInt(req.query.p) : 1;
    let sumAllFiles = req.query.s ? parseInt(req.query.s) : 0;

    const dataByMonthFolder  = 'src/files/saved_data/months/';    
    //pointers vars
    let pointerPrev1    = {id:'', lat:'', lon:'', delta:'', time:''};
    let pointerNow      = { ...pointerPrev1 };//current user
    let prevVisitor     = { ...pointerPrev1 };//prev first occurence of current user
    let currVisitor     = { ...pointerPrev1 };//current user
    //processing vars
    let objVisits       = {};
    let objVisitors     = {};
    let sumVisitTime    = 0;
    let betweenVisits   = {};
    let countNbBetween  = 0;
    let sumBetweenVisit = 0;
    let sumDistance     = 0;
    let avgSpeed        = 0;
    let countVisit      = 0;
    let listAllFiles    = [ ...Array(maxPart).keys() ].map( i => i + 1);
    let jsonResult      = {};

    if(sumAllFiles === 0){
        const folderCsvMonth = `${dataByMonthFolder}`;
        fs.readdir(folderCsvMonth, (err, files) => {
            files.forEach(file => {
                sumAllFiles += fs.statSync(`${dataByMonthFolder}${file}`).size;
            });
        });
    }

    const loopStream = (filePath) => new Promise((resolve, reject) => {
        let readStream   = fs.createReadStream(filePath /*,{ highWaterMark: 8 * 1024 }*/);  

        readStream.on('data', (chunk) => {
            const csvTab = chunk.toString().split('\n');
            entries   += csvTab.length;
            allChunk  += chunk.length;
            
            for(const prop in csvTab){
                const dataLine = csvTab[prop].split(',');
                let [id, lat, lon, delta, unix_timestamp] = dataLine;

                if(id.length > 1){
                    pointerPrev1 = pointerNow;
                    pointerNow   = { id, lat, lon, delta, unix_timestamp };

                    objVisitors[id] = !Object.hasOwn(objVisitors, id) ? 1 : objVisitors[id];
                    objVisits[id]   = !Object.hasOwn(objVisits, id)   ? 1 : objVisits[id] + 1;                

                    if(currVisitor.id !== id){
                        /**
                         *  2 lines of the same user.
                         *  prevVisitor 1st occurence of user.
                         *  pointerPrev1 last occurence of user.
                        **/
                        prevVisitor = currVisitor;
                        currVisitor = { id, lat, lon, delta, unix_timestamp };

                        if(prevVisitor.id.length > 1){
                            //range from time [first] to last time [last]
                            let numberTimeStampStart = parseInt(prevVisitor.unix_timestamp);
                            let numberTimeStampEnd = parseInt(pointerPrev1.unix_timestamp);
                            if(isValidTimeStamp(numberTimeStampStart) && isValidTimeStamp(numberTimeStampEnd)){                            
                                countVisit   += 1;
                                sumVisitTime += Math.abs(numberTimeStampStart - numberTimeStampEnd);//seconds

                                const distance = getDistanceBetweenPoints(prevVisitor.lat, prevVisitor.lon, pointerPrev1.lat, pointerPrev1.lon, 'meters');
                                sumDistance   += !Number.isNaN(distance) ? distance : 0;                                                                
                                avgSpeed       = sumVisitTime > 0 ? sumDistance / sumVisitTime : 0;//seconds
                            }
                            
                            if(!Object.hasOwn(betweenVisits, pointerPrev1.id)){//last occurence/last time seen
                                betweenVisits[pointerPrev1.id] = { id: pointerPrev1.id, unix_timestamp: pointerPrev1.unix_timestamp};
                            }else
                            if(Object.hasOwn(betweenVisits, currVisitor.id) && betweenVisits[currVisitor.id].id === currVisitor.id){//first time seen
                                numberTimeStampStart = parseInt(betweenVisits[currVisitor.id].unix_timestamp);
                                numberTimeStampEnd = parseInt(currVisitor.unix_timestamp);
                                if(isValidTimeStamp(numberTimeStampStart) && isValidTimeStamp(numberTimeStampEnd)){
                                    let daysDiff = Math.abs((numberTimeStampStart - numberTimeStampEnd) / TIME_STAMP_TO_DAYS);
                                    if(daysDiff > 0){//is is a day length in time
                                        sumBetweenVisit += daysDiff;
                                        countNbBetween++;
                                    }
                                    delete betweenVisits[currVisitor.id];
                                }
                            }
                        }
                    }
                }
            }
            jsonResult = {
                fileSize: sumAllFiles,
                lineCount: entries,
                chunkSize: allChunk,
                avgVisitTime: parseInt(sumVisitTime / countVisit),
                avgSpeed,
                sumBetweenVisit,
                countNbBetween,
                visits: countVisit,
                visitors: Object.keys(objVisitors).length,
            }
            res.write(JSON.stringify(jsonResult));
        });
        readStream.on('end', (chunk) => {
            resolve();
        });
    });
           
    let cntLoop = 1;
    listAllFiles.forEach(async fileNumb => { 
        const file = `${dataByMonthFolder}data_month_${month}_${fileNumb}.csv`;     
        await loopStream(file).then(() => {
            if(cntLoop === listAllFiles.length){
                res.end(JSON.stringify(jsonResult));
            }
        });                   
        cntLoop++;
    });
});

module.exports = {
    getFileByMonth,
    getMainFile
}
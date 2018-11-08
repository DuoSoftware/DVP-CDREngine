var amqp = require('amqp');
var config = require('config');
var dbHandler = require('./DBBackendHandler.js');
var amqpPublisher = require('./AMQPPublisher.js').PublishToQueue;
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;
var redisHandler = require('./RedisHandler.js');
var async = require('async');


var addRedisObjects = function(mainLegId, objList, callback)
{
    redisHandler.AddSetWithExpire('UUID_MAP_' + mainLegId, objList, 86400, function(addSetErr, addSetRes)
    {
        var tempKeyValPair = {};

        objList.forEach(function(obj)
        {
            tempKeyValPair[obj] = mainLegId;
        });

        redisHandler.MSetObject(tempKeyValPair, function(mSetErr, mSetRes)
        {
            objList.forEach(function(expireKey)
            {
                redisHandler.ExpireKey(expireKey, 86400);
            })

            callback(null, true);

        })


    });
};

var processBLegs = function(legInfo, cdrListArr, callback)
{
    var legType = legInfo.ObjType;

    if(legType && (legType === 'ATT_XFER_USER' || legType === 'ATT_XFER_GATEWAY'))
    {
        //check for Originated Legs

        if(legInfo.OriginatedLegs)
        {
            var decodedLegsStr = decodeURIComponent(legInfo.OriginatedLegs);

            var formattedStr = decodedLegsStr.replace("ARRAY::", "");

            var legsUnformattedList = formattedStr.split('|:');

            if(legsUnformattedList && legsUnformattedList.length > 0)
            {
                var legProperties = legsUnformattedList[0].split(';');

                var legUuid = legProperties[0];

                dbHandler.GetSpecificLegByUuid(legUuid, function (err, transferLeg)
                {
                    cdrListArr.push(legInfo);

                    if(transferLeg)
                    {
                        var tempTransLeg = transferLeg.toJSON();
                        tempTransLeg.IsTransferredParty = true;
                        cdrListArr.push(tempTransLeg);
                        callback(null, null);
                    }
                    else
                    {
                        callback(null, 'UUID_' + legUuid);
                    }

                });


            }
            else
            {
                cdrListArr.push(legInfo);
                callback(null, null);
            }
        }
        else
        {
            cdrListArr.push(legInfo);
            callback(null, null);
        }

    }
    else
    {
        cdrListArr.push(legInfo);
        callback(null, null);
    }
};

var collectBLegs = function(cdrListArr, uuid, callUuid, callback)
{
    var actionObj = {};
    dbHandler.GetBLegsForIVRCalls(uuid, callUuid, function(err, legInfo)
    {
        if(legInfo && legInfo.length > 0)
        {
            //DATA FOUND NEED TO COMPARE

            var asyncArr = [];

            for(i=0; i<legInfo.length; i++)
            {
                asyncArr.push(processBLegs.bind(this, legInfo[i], cdrListArr));
            }

            async.parallel(asyncArr, function(err, missingOriginatedLegs)
            {

                var cleanedUpList = missingOriginatedLegs.filter(function(missingLeg) { return missingLeg });
                cleanedUpList.push('CALL_UUID_' + callUuid);

                redisHandler.GetSetMembers('UUID_MAP_' + uuid, function(err, missingLegsRedis)
                {
                    if(missingLegsRedis && missingLegsRedis.length > 0)
                    {
                        var allItemsAlreadyAdded = true;
                        cleanedUpList.forEach(function(missingOrigLeg)
                        {
                            var index = missingLegsRedis.indexOf(missingOrigLeg);
                            if (index <= -1) {
                                allItemsAlreadyAdded = false;
                            }

                        });

                        if(!allItemsAlreadyAdded)
                        {
                            //ADD TO REDIS
                            addRedisObjects(uuid, cleanedUpList, function(err, redisAddResult){

                                //CALLBACK
                                actionObj.AddToQueue = true;
                                actionObj.RemoveFromRedis = false;
                                actionObj.SaveOnDB = true;

                                callback(null, actionObj);

                            });

                        }
                        else
                        {
                            actionObj.AddToQueue = false;
                            actionObj.RemoveFromRedis = false;
                            actionObj.SaveOnDB = true;

                            callback(null, actionObj);
                        }


                    }
                    else
                    {
                        //ADD TO REDIS

                        addRedisObjects(uuid, cleanedUpList, function(err, redisAddResult){

                            //CALLBACK
                            actionObj.AddToQueue = true;
                            actionObj.RemoveFromRedis = false;
                            actionObj.SaveOnDB = true;

                            callback(null, actionObj);

                        });
                    }

                });
            });
        }
        else
        {
            var simpleBLegArr = [];
            simpleBLegArr.push('CALL_UUID_' + callUuid);

            redisHandler.GetSetMembers('UUID_MAP_' + uuid, function(err, missingLegsRedis)
            {
                if(missingLegsRedis && missingLegsRedis.length > 0)
                {
                    var allItemsAlreadyAdded = true;
                    var index = missingLegsRedis.indexOf('CALL_UUID_' + callUuid);
                    if (index <= -1) {
                        allItemsAlreadyAdded = false;
                    }

                    if(!allItemsAlreadyAdded)
                    {
                        //ADD TO REDIS
                        addRedisObjects(uuid, simpleBLegArr, function(err, redisAddResult){

                            //CALLBACK
                            actionObj.AddToQueue = true;
                            actionObj.RemoveFromRedis = false;
                            actionObj.SaveOnDB = true;

                            callback(null, actionObj);

                        });

                    }
                    else
                    {
                        actionObj.AddToQueue = false;
                        actionObj.RemoveFromRedis = false;
                        actionObj.SaveOnDB = true;

                        callback(null, actionObj);
                    }

                }
                else
                {
                    //ADD TO REDIS

                    addRedisObjects(uuid, simpleBLegArr, function(err, redisAddResult){

                        //CALLBACK
                        actionObj.AddToQueue = true;
                        actionObj.RemoveFromRedis = false;
                        actionObj.SaveOnDB = true;

                        callback(null, actionObj);

                    });
                }

            });
        }
    })
};

var processOriginatedLegs = function(legInfo, cdrListArr, callback)
{
    var legType = legInfo.ObjType;

    if(legType && (legType === 'ATT_XFER_USER' || legType === 'ATT_XFER_GATEWAY'))
    {
        if(legInfo.OriginatedLegs)
        {
            var decodedLegsStr = decodeURIComponent(legInfo.OriginatedLegs);

            var formattedStr = decodedLegsStr.replace("ARRAY::", "");

            var legsUnformattedList = formattedStr.split('|:');

            if (legsUnformattedList && legsUnformattedList.length > 0)
            {
                var legProperties = legsUnformattedList[0].split(';');

                var legUuid = legProperties[0];

                dbHandler.GetSpecificLegByUuid(legUuid, function (err, transferLeg)
                {
                    cdrListArr.push(legInfo);

                    if(transferLeg)
                    {
                        var tempTransLeg = transferLeg.toJSON();
                        tempTransLeg.IsTransferredParty = true;
                        cdrListArr.push(tempTransLeg);
                        callback(null, null);
                    }
                    else
                    {
                        callback(null, 'UUID_' + legUuid);
                    }

                })
            }
            else
            {
                cdrListArr.push(legInfo);
                callback(null, null);
            }
        }
        else
        {
            cdrListArr.push(legInfo);
            callback(null, null);
        }
    }
    else
    {
        cdrListArr.push(legInfo);
        callback(null, null);
    }
};

var collectOtherLegsCDR = function(cdrListArr, relatedLegs, tryCount, mainLegId, callback)
{
    var actionObj = {};
    dbHandler.GetSpecificLegsByUuids(relatedLegs, function(allLegsFound, objList)
    {
        if(allLegsFound)
        {
            //LOOP THROUGH LEGS LIST

            var asyncArr = [];

            objList.forEach(function(legInfo)
            {
                asyncArr.push(processOriginatedLegs.bind(this, legInfo, cdrListArr));
            });

            async.parallel(asyncArr, function(err, missingLegsList){
                if(missingLegsList)
                {
                    //CHECK MISSING LEG ALREADY ADDED
                    var cleanedUpList = missingLegsList.filter(function(missingLeg) { return missingLeg });

                    if(cleanedUpList && cleanedUpList.length > 0)
                    {
                        //DO REDIS COMPARE

                        redisHandler.GetSetMembers('UUID_MAP_' + mainLegId, function(err, missingLegsRedis)
                        {
                            if(missingLegsRedis && missingLegsRedis.length > 0)
                            {
                                var allItemsAlreadyAdded = true;
                                cleanedUpList.forEach(function(missingOrigLeg)
                                {
                                    var index = missingLegsRedis.indexOf(missingOrigLeg);
                                    if (index <= -1) {
                                        allItemsAlreadyAdded = false;
                                    }

                                });

                                if(!allItemsAlreadyAdded)
                                {
                                    //ADD TO REDIS
                                    addRedisObjects(mainLegId, cleanedUpList, function(err, redisAddResult){

                                        //CALLBACK
                                        actionObj.AddToQueue = true;
                                        actionObj.RemoveFromRedis = false;
                                        actionObj.SaveOnDB = true;

                                        callback(null, actionObj);

                                    });

                                }
                                else
                                {
                                    actionObj.AddToQueue = false;
                                    actionObj.RemoveFromRedis = false;
                                    actionObj.SaveOnDB = true;

                                    callback(null, actionObj);
                                }


                            }
                            else
                            {
                                //ADD TO REDIS
                                addRedisObjects(mainLegId, cleanedUpList, function(err, redisAddResult){

                                    //CALLBACK
                                    actionObj.AddToQueue = true;
                                    actionObj.RemoveFromRedis = false;
                                    actionObj.SaveOnDB = true;

                                    callback(null, actionObj);

                                });
                            }

                        });
                    }
                    else
                    {
                        //ALL OK PROCESS

                        actionObj.AddToQueue = false;
                        actionObj.RemoveFromRedis = true;
                        actionObj.SaveOnDB = true;

                        callback(null, actionObj);

                    }


                }
            });

        }
        else
        {
            //PUT BACK TO QUEUE

            redisHandler.GetSetMembers('UUID_MAP_' + mainLegId, function(err, items)
            {
                //If redis has already added items no need to compare set to remove from queue

                if(items && items.length > 0)
                {
                    actionObj.AddToQueue = false;
                    actionObj.RemoveFromRedis = false;
                    actionObj.SaveOnDB = true;

                    callback(null, actionObj);
                }
                else
                {
                    //ADD TO REDIS
                    addRedisObjects(mainLegId, objList, function(err, addResult)
                    {

                        actionObj.AddToQueue = true;
                        actionObj.RemoveFromRedis = false;
                        actionObj.SaveOnDB = true;

                        callback(null, actionObj);

                    });

                }


            });
        }

    });

};

var processCDRLegs = function(processedCdr, cdrList, callback)
{
    cdrList[processedCdr.Uuid] = [];
    cdrList[processedCdr.Uuid].push(processedCdr);

    var relatedLegsLength = 0;

    if(processedCdr.RelatedLegs)
    {
        relatedLegsLength = processedCdr.RelatedLegs.length;
    }

    if(processedCdr.RelatedLegs && relatedLegsLength)
    {
        console.log('=============== ORIGINATED LEGS PROCESSING =================');
        collectOtherLegsCDR(cdrList[processedCdr.Uuid], processedCdr.RelatedLegs, processedCdr.TryCount, processedCdr.Uuid, function(err, actionObj)
        {
            //Response will return false if leg need to be added back to queue
            callback(null, cdrList, actionObj);

        })
    }
    else
    {
        if(processedCdr.ObjType === 'HTTAPI' || processedCdr.ObjType === 'SOCKET' || processedCdr.ObjCategory === 'DIALER')
        {
            console.log('=============== HTTAPI LEGS PROCESSING =================');
            collectBLegs(cdrList[processedCdr.Uuid], processedCdr.Uuid, processedCdr.CallUuid, function(err, actionObj)
            {
                callback(null, cdrList, actionObj);
            })

        }
        else
        {
            console.log('================== UNKNOWN TYPE LEG CALL FOUND PROCESSING AS IT IS - UUID : ' + processedCdr.Uuid + ' ==================');

            var tempActionObj1 = {
                AddToQueue: false,
                RemoveFromRedis: false,
                SaveOnDB: true
            };
            callback(null, cdrList, tempActionObj1);
        }

    }

};

var decodeOriginatedLegs = function(cdr)
{

    try
    {
        var OriginatedLegs = cdr.OriginatedLegs;

        if(OriginatedLegs){

            //Do HTTP DECODE
            var decodedLegsStr = decodeURIComponent(OriginatedLegs);

            var formattedStr = decodedLegsStr.replace("ARRAY::", "");

            var legsUnformattedList = formattedStr.split('|:');

            cdr.RelatedLegs = [];

            for(j=0; j<legsUnformattedList.length; j++){

                var legProperties = legsUnformattedList[j].split(';');

                var legUuid = legProperties[0];

                if(cdr.Uuid != legUuid && !(cdr.RelatedLegs.indexOf(legUuid) > -1)){

                    cdr.RelatedLegs.push(legUuid);
                }

            }
        }

        return cdr;
    }
    catch(ex)
    {
        return null;
    }
};

var processCampaignCDR = function(primaryLeg, curCdr)
{
    var cdrAppendObj = {};
    var callHangupDirectionA = '';
    var callHangupDirectionB = '';
    var isOutboundTransferCall = false;
    var holdSecTemp = 0;

    var callCategory = '';


    //Need to filter out inbound and outbound legs before processing

    var firstLeg = primaryLeg;

    if(firstLeg)
    {
        //Process First Leg
        callHangupDirectionA = firstLeg.HangupDisposition;

        if(firstLeg.ObjType === 'ATT_XFER_USER' || firstLeg.ObjType === 'ATT_XFER_GATEWAY')
        {
            isOutboundTransferCall = true;
        }

        cdrAppendObj.Uuid = firstLeg.Uuid;
        cdrAppendObj.RecordingUuid = firstLeg.Uuid;
        cdrAppendObj.CallUuid = firstLeg.CallUuid;
        cdrAppendObj.BridgeUuid = firstLeg.BridgeUuid;
        cdrAppendObj.SwitchName = firstLeg.SwitchName;
        cdrAppendObj.SipFromUser = firstLeg.SipFromUser;
        cdrAppendObj.SipToUser = firstLeg.SipToUser;
        cdrAppendObj.RecievedBy = firstLeg.SipToUser;
        cdrAppendObj.CallerContext = firstLeg.CallerContext;
        cdrAppendObj.HangupCause = firstLeg.HangupCause;
        cdrAppendObj.CreatedTime = firstLeg.CreatedTime;
        cdrAppendObj.Duration = firstLeg.Duration;
        cdrAppendObj.BridgedTime = firstLeg.BridgedTime;
        cdrAppendObj.HangupTime = firstLeg.HangupTime;
        cdrAppendObj.AppId = firstLeg.AppId;
        cdrAppendObj.CompanyId = firstLeg.CompanyId;
        cdrAppendObj.TenantId = firstLeg.TenantId;
        cdrAppendObj.ExtraData = firstLeg.ExtraData;
        cdrAppendObj.IsQueued = firstLeg.IsQueued;
        cdrAppendObj.IsAnswered = false;
        cdrAppendObj.CampaignName = firstLeg.CampaignName;
        cdrAppendObj.CampaignId = firstLeg.CampaignId;
        cdrAppendObj.BillSec = 0;
        cdrAppendObj.HoldSec = 0;
        cdrAppendObj.ProgressSec = 0;
        cdrAppendObj.FlowBillSec = 0;
        cdrAppendObj.ProgressMediaSec = 0;
        cdrAppendObj.WaitSec = 0;
        cdrAppendObj.AnswerSec = 0;

        holdSecTemp = holdSecTemp + firstLeg.HoldSec;

        if(firstLeg.ObjType === 'BLAST' || firstLeg.ObjType === 'DIRECT' || firstLeg.ObjType === 'IVRCALLBACK')
        {
            cdrAppendObj.BillSec = firstLeg.BillSec;
            cdrAppendObj.AnswerSec = firstLeg.AnswerSec;
            callHangupDirectionB = firstLeg.HangupDisposition;
            cdrAppendObj.IsAnswered = firstLeg.IsAnswered;
        }
        if(firstLeg.ObjType === 'AGENT')
        {
            cdrAppendObj.AgentAnswered = firstLeg.IsAnswered;
        }

        cdrAppendObj.DVPCallDirection = 'outbound';


        if(firstLeg.ProgressSec)
        {
            cdrAppendObj.ProgressSec = firstLeg.ProgressSec;
        }

        if(firstLeg.FlowBillSec)
        {
            cdrAppendObj.FlowBillSec = firstLeg.FlowBillSec;
        }

        if(firstLeg.ProgressMediaSec)
        {
            cdrAppendObj.ProgressMediaSec = firstLeg.ProgressMediaSec;
        }

        if(firstLeg.WaitSec)
        {
            cdrAppendObj.WaitSec = firstLeg.WaitSec;
        }

        cdrAppendObj.QueueSec = firstLeg.QueueSec;
        cdrAppendObj.AgentSkill = firstLeg.AgentSkill;

        cdrAppendObj.AnswerSec = firstLeg.AnswerSec;
        cdrAppendObj.AnsweredTime = firstLeg.AnsweredTime;

        cdrAppendObj.ObjType = firstLeg.ObjType;
        cdrAppendObj.ObjCategory = firstLeg.ObjCategory;
    }

    //process other legs

    var otherLegs = curCdr.filter(function (item) {
        return item.ObjCategory !== 'DIALER';

    });

    if(otherLegs && otherLegs.length > 0)
    {
        var customerLeg = otherLegs.find(function (item) {
            return item.ObjType === 'CUSTOMER';
        });

        var agentLeg = otherLegs.find(function (item) {
            return (item.ObjType === 'AGENT' || item.ObjType === 'PRIVATE_USER');
        });

        if(customerLeg)
        {
            cdrAppendObj.BillSec = customerLeg.BillSec;
            cdrAppendObj.AnswerSec = customerLeg.AnswerSec;

            holdSecTemp = holdSecTemp + customerLeg.HoldSec;

            callHangupDirectionB = customerLeg.HangupDisposition;

            cdrAppendObj.IsAnswered = customerLeg.IsAnswered;

            cdrAppendObj.IsQueued = customerLeg.IsQueued;

        }

        if(agentLeg)
        {
            holdSecTemp = holdSecTemp + agentLeg.HoldSec;
            callHangupDirectionB = agentLeg.HangupDisposition;
            cdrAppendObj.RecievedBy = agentLeg.SipToUser;

            if(firstLeg.ObjType !== 'AGENT')
            {
                cdrAppendObj.AgentAnswered = agentLeg.IsAnswered;
            }
        }

        cdrAppendObj.HoldSec = holdSecTemp;
        cdrAppendObj.IvrConnectSec = 0;

    }

    if(!cdrAppendObj.IsAnswered)
    {
        cdrAppendObj.AnswerSec = cdrAppendObj.Duration;
    }


    if (callHangupDirectionA === 'recv_bye') {
        cdrAppendObj.HangupParty = 'CALLER';
    }
    else if (callHangupDirectionB === 'recv_bye') {
        cdrAppendObj.HangupParty = 'CALLEE';
    }
    else {
        cdrAppendObj.HangupParty = 'SYSTEM';
    }


    return cdrAppendObj;

};

var processSingleCdrLeg = function(primaryLeg, callback)
{
    var cdr = decodeOriginatedLegs(primaryLeg);

    var cdrList = {};

    //processCDRLegs method should immediately stop execution and return missing leg uuids or return the related cdr legs
    processCDRLegs(cdr, cdrList, function(err, resp, actionObj)
    {
        if(err)
        {
            logger.error('[DVP-CDRProcessor.processSingleCdrLeg] - [%s] - Error occurred while processing CDR Legs', err);
        }
        if(actionObj.SaveOnDB)
        {
            var cdrAppendObj = {};
            var primaryLeg = cdr;
            var isOutboundTransferCall = false;

            if(primaryLeg.DVPCallDirection === 'outbound' && (primaryLeg.ObjType === 'ATT_XFER_USER' || primaryLeg.ObjType === 'ATT_XFER_GATEWAY'))
            {
                isOutboundTransferCall = true;
            }

            var curCdr = resp[Object.keys(resp)[0]];

            if(cdr.ObjCategory === 'DIALER')
            {
                cdrAppendObj =  processCampaignCDR(primaryLeg, curCdr);
            }
            else
            {
                var outLegAnswered = false;

                var callHangupDirectionA = '';
                var callHangupDirectionB = '';

                //Need to filter out inbound and outbound legs before processing

                /*var filteredInb = curCdr.filter(function (item)
                 {
                 if (item.Direction === 'inbound')
                 {
                 return true;
                 }
                 else
                 {
                 return false;
                 }

                 });*/

                var secondaryLeg = null;

                var filteredOutb = curCdr.filter(function (item)
                {
                    return item.Direction === 'outbound';
                });


                var transferredParties = '';

                var transferCallOriginalCallLeg = null;

                var transLegInfo = {
                    transferLegB: [],
                    actualTransferLegs: []
                };

                filteredOutb.reduce(function(accumilator, currentValue)
                {
                    if(isOutboundTransferCall)
                    {
                        if (currentValue.ObjType !== 'ATT_XFER_USER' && currentValue.ObjType !== 'ATT_XFER_GATEWAY')
                        {
                            accumilator.transferLegB.push(currentValue);
                        }
                        else
                        {
                            accumilator.actualTransferLegs.push(currentValue);
                        }

                    }
                    else
                    {
                        if ((currentValue.ObjType === 'ATT_XFER_USER' || currentValue.ObjType === 'ATT_XFER_GATEWAY') && !currentValue.IsTransferredParty)
                        {
                            accumilator.transferLegB.push(currentValue);
                        }

                        if(currentValue.IsTransferredParty)
                        {
                            accumilator.actualTransferLegs.push(currentValue);
                        }
                    }
                    return accumilator;

                }, transLegInfo);

                if(transLegInfo && transLegInfo.actualTransferLegs && transLegInfo.actualTransferLegs.length > 0 && transLegInfo.transferLegB && transLegInfo.transferLegB.length > 0)
                {
                    transLegInfo.actualTransferLegs.forEach(function(actualTransLeg)
                    {
                        var index = transLegInfo.transferLegB.map(function(e) { return e.Uuid }).indexOf(actualTransLeg.Uuid);

                        if(index > -1)
                        {
                            transLegInfo.transferLegB.splice(index, 1);
                        }

                    })
                }


                if(transLegInfo.transferLegB && transLegInfo.transferLegB.length > 0)
                {

                    var transferLegBAnswered = transLegInfo.transferLegB.filter(function (item) {
                        return item.IsAnswered === true;
                    });

                    if(transferLegBAnswered && transferLegBAnswered.length > 0)
                    {
                        transferCallOriginalCallLeg = transferLegBAnswered[0];
                    }
                    else
                    {
                        transferCallOriginalCallLeg = transLegInfo.transferLegB[0];
                    }
                }

                var callCategory = primaryLeg.ObjCategory;

                if(transferCallOriginalCallLeg)
                {
                    secondaryLeg = transferCallOriginalCallLeg;

                    for(k = 0; k < transLegInfo.actualTransferLegs.length; k++)
                    {
                        transferredParties = transferredParties + transLegInfo.actualTransferLegs[k].SipToUser + ',';
                    }
                }
                else
                {
                    if(filteredOutb.length > 1)
                    {
                        var filteredOutbAnswered = filteredOutb.filter(function (item2)
                        {
                            return item2.IsAnswered;
                        });

                        if(filteredOutbAnswered && filteredOutbAnswered.length > 0)
                        {
                            if(filteredOutbAnswered.length > 1)
                            {
                                //check for bridged calles
                                var filteredOutbBridged = filteredOutbAnswered.filter(function (item3)
                                {
                                    return item3.BridgedTime > new Date('1970-01-01');
                                });

                                if(filteredOutbBridged && filteredOutbBridged.length > 0)
                                {
                                    secondaryLeg = filteredOutbBridged[0];
                                }
                                else
                                {
                                    secondaryLeg = filteredOutbAnswered[0];
                                }

                            }
                            else
                            {
                                secondaryLeg = filteredOutbAnswered[0];
                            }

                        }
                        else
                        {
                            secondaryLeg = filteredOutb[0];
                        }
                    }
                    else
                    {
                        if(filteredOutb && filteredOutb.length > 0)
                        {
                            secondaryLeg = filteredOutb[0];

                            if(callCategory === 'FOLLOW_ME' || callCategory === 'FORWARDING')
                            {
                                for (k = 0; k < filteredOutb.length; k++) {
                                    transferredParties = transferredParties + filteredOutb[k].SipToUser + ',';
                                }

                            }


                        }
                    }
                }

                if(cdrAppendObj.ObjType === 'FAX_INBOUND')
                {
                    cdrAppendObj.IsAnswered = primaryLeg.IsAnswered;
                }

                //process primary leg first

                //process common data

                cdrAppendObj.Uuid = primaryLeg.Uuid;
                cdrAppendObj.RecordingUuid = primaryLeg.Uuid;
                cdrAppendObj.CallUuid = primaryLeg.CallUuid;
                cdrAppendObj.BridgeUuid = primaryLeg.BridgeUuid;
                cdrAppendObj.SwitchName = primaryLeg.SwitchName;
                cdrAppendObj.SipFromUser = primaryLeg.SipFromUser;
                cdrAppendObj.SipToUser = primaryLeg.SipToUser;
                cdrAppendObj.CallerContext = primaryLeg.CallerContext;
                cdrAppendObj.HangupCause = primaryLeg.HangupCause;
                cdrAppendObj.CreatedTime = primaryLeg.CreatedTime;
                cdrAppendObj.Duration = primaryLeg.Duration;
                cdrAppendObj.BridgedTime = primaryLeg.BridgedTime;
                cdrAppendObj.HangupTime = primaryLeg.HangupTime;
                cdrAppendObj.AppId = primaryLeg.AppId;
                cdrAppendObj.CompanyId = primaryLeg.CompanyId;
                cdrAppendObj.TenantId = primaryLeg.TenantId;
                cdrAppendObj.ExtraData = primaryLeg.ExtraData;
                cdrAppendObj.IsQueued = primaryLeg.IsQueued;
                cdrAppendObj.BusinessUnit = primaryLeg.BusinessUnit;

                cdrAppendObj.AgentAnswered = primaryLeg.AgentAnswered;

                if (primaryLeg.DVPCallDirection)
                {
                    callHangupDirectionA = primaryLeg.HangupDisposition;
                }

                cdrAppendObj.IsAnswered = false;


                cdrAppendObj.BillSec = 0;
                cdrAppendObj.HoldSec = 0;
                cdrAppendObj.ProgressSec = 0;
                cdrAppendObj.FlowBillSec = 0;
                cdrAppendObj.ProgressMediaSec = 0;
                cdrAppendObj.WaitSec = 0;

                if(primaryLeg.ProgressSec)
                {
                    cdrAppendObj.ProgressSec = primaryLeg.ProgressSec;
                }

                if(primaryLeg.FlowBillSec)
                {
                    cdrAppendObj.FlowBillSec = primaryLeg.FlowBillSec;
                }

                if(primaryLeg.ProgressMediaSec)
                {
                    cdrAppendObj.ProgressMediaSec = primaryLeg.ProgressMediaSec;
                }

                if(primaryLeg.WaitSec)
                {
                    cdrAppendObj.WaitSec = primaryLeg.WaitSec;
                }


                cdrAppendObj.DVPCallDirection = primaryLeg.DVPCallDirection;

                cdrAppendObj.HoldSec = cdrAppendObj.HoldSec +  primaryLeg.HoldSec;

                /*if (primaryLeg.DVPCallDirection === 'inbound')
                 {
                 cdrAppendObj.HoldSec = primaryLeg.HoldSec;
                 }*/


                cdrAppendObj.QueueSec = primaryLeg.QueueSec;
                cdrAppendObj.AgentSkill = primaryLeg.AgentSkill;

                cdrAppendObj.AnswerSec = primaryLeg.AnswerSec;
                cdrAppendObj.AnsweredTime = primaryLeg.AnsweredTime;

                cdrAppendObj.ObjType = primaryLeg.ObjType;
                cdrAppendObj.ObjCategory = primaryLeg.ObjCategory;
                cdrAppendObj.TimeAfterInitialBridge = primaryLeg.TimeAfterInitialBridge;

                //process outbound legs next

                if(secondaryLeg)
                {
                    if (secondaryLeg.BillSec > 0)
                    {
                        outLegAnswered = true;
                    }

                    if(cdrAppendObj.DVPCallDirection === 'inbound' && outLegAnswered)
                    {
                        cdrAppendObj.BillSec = primaryLeg.Duration - primaryLeg.TimeAfterInitialBridge;
                    }

                    if(cdrAppendObj.DVPCallDirection === 'outbound')
                    {
                        cdrAppendObj.RecordingUuid = secondaryLeg.Uuid;
                    }

                    callHangupDirectionB = secondaryLeg.HangupDisposition;

                    cdrAppendObj.RecievedBy = secondaryLeg.SipToUser;

                    cdrAppendObj.AnsweredTime = secondaryLeg.AnsweredTime;


                    cdrAppendObj.HoldSec = cdrAppendObj.HoldSec + secondaryLeg.HoldSec;
                    /*if (primaryLeg.DVPCallDirection === 'outbound')
                     {
                     cdrAppendObj.HoldSec = secondaryLeg.HoldSec;
                     }*/

                    if(cdrAppendObj.DVPCallDirection === 'outbound')
                    {
                        cdrAppendObj.BillSec = secondaryLeg.BillSec;
                    }

                    if (!cdrAppendObj.ObjType)
                    {
                        cdrAppendObj.ObjType = secondaryLeg.ObjType;
                    }

                    if (!cdrAppendObj.ObjCategory)
                    {
                        cdrAppendObj.ObjCategory = secondaryLeg.ObjCategory;
                    }

                    cdrAppendObj.AnswerSec = secondaryLeg.AnswerSec;

                    if(!outLegAnswered && cdrAppendObj.RecievedBy)
                    {
                        cdrAppendObj.AnswerSec = secondaryLeg.Duration;
                    }

                    if(transferredParties)
                    {
                        transferredParties = transferredParties.slice(0, -1);
                        cdrAppendObj.TransferredParties = transferredParties;
                    }
                }

                /*if(transferCallOriginalCallLeg)
                 {
                 cdrAppendObj.SipFromUser = transferCallOriginalCallLeg.SipFromUser;
                 }*/


                cdrAppendObj.IvrConnectSec = cdrAppendObj.Duration - cdrAppendObj.QueueSec - cdrAppendObj.BillSec;


                cdrAppendObj.IsAnswered = outLegAnswered;


                if (callHangupDirectionA === 'recv_bye' || callHangupDirectionA === 'recv_cancel')
                {
                    cdrAppendObj.HangupParty = 'CALLER';
                }
                else if (callHangupDirectionB === 'recv_bye' || callHangupDirectionB === 'recv_refuse')
                {
                    cdrAppendObj.HangupParty = 'CALLEE';
                }
                else
                {
                    cdrAppendObj.HangupParty = 'SYSTEM';
                }
            }

            dbHandler.AddProcessedCDR(cdrAppendObj, function(err, addResp)
            {
                callback(err, actionObj);

            });
        }
        else
        {
            callback(null, actionObj);
        }


    })
};

var ips = [];
if(config.RabbitMQ.ip) {
    ips = config.RabbitMQ.ip.split(",");
}


var connection = amqp.createConnection({
    //url: queueHost,
    host: ips,
    port: config.RabbitMQ.port,
    login: config.RabbitMQ.user,
    password: config.RabbitMQ.password,
    vhost: config.RabbitMQ.vhost,
    noDelay: true,
    heartbeat:10
}, {
    reconnect: true,
    reconnectBackoffStrategy: 'linear',
    reconnectExponentialLimit: 120000,
    reconnectBackoffTime: 1000
});

connection.on('connect', function()
{
    logger.debug('[DVP-CDRProcessor.AMQPConnection] - [%s] - AMQP Connection CONNECTED');
});

connection.on('ready', function()
{
    amqpConState = 'READY';

    logger.debug('[DVP-CDRProcessor.AMQPConnection] - [%s] - AMQP Connection READY');

    connection.queue('CDRQUEUE', {durable: true, autoDelete: false}, function (q) {
        q.bind('#');

        // Receive messages
        q.subscribe(function (message) {

            console.log('================ CDR RECEIVED FROM QUEUE - UUID : ' + message.Uuid + ' ================');

            processSingleCdrLeg(message, function(err, actionObj)
            {
                if(actionObj.RemoveFromRedis)
                {
                    //REMOVE REDIS OBJECTS
                    redisHandler.GetSetMembers('UUID_MAP_' + message.Uuid, function(err, items)
                    {
                        items.forEach(function(item){
                            redisHandler.DeleteObject(item);
                        });

                        redisHandler.DeleteObject('UUID_MAP_' + message.Uuid);

                    });
                }

                if(actionObj.AddToQueue)
                {
                    message.TryCount++;

                    setTimeout(amqpPublisher, 3000, 'CDRQUEUE', message);

                }
            })


        });

    });
});

connection.on('error', function(e)
{
    logger.error('[DVP-EventMonitor.handler] - [%s] - AMQP Connection ERROR', e);
    amqpConState = 'CLOSE';
});


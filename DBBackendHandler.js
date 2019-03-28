var dbModel = require('dvp-dbmodels');
var redlock = require('./RedisHandler.js').redlock;
var amqpPublisher = require('./AMQPPublisher.js').PublishToQueue;
var config = require('config');

var GetSpecificLegsByUuids = function(uuidList, callback)
{
    try
    {
        var query = {where :[{$or: []}]};

        var tempUuidList = [];

        uuidList.forEach(function(uuid)
        {
            tempUuidList.push('UUID_' + uuid);
            query.where[0].$or.push({Uuid: uuid})
        });

        dbModel.CallCDR.findAll(query).then(function(callLegs)
        {
            if(callLegs.length > 0)
            {
                callLegs.forEach(function(callLeg)
                {
                    var index = tempUuidList.indexOf('UUID_' + callLeg.Uuid);
                    if (index > -1) {
                        tempUuidList.splice(index, 1);
                    }
                });

                if(tempUuidList.length > 0)
                {
                    callback(false, tempUuidList);
                }
                else
                {
                    callback(true, callLegs);
                }

            }
            else
            {
                callback(false, tempUuidList);
            }

        });

    }
    catch(ex)
    {
        callback(false, uuidList);
    }
};

var GetSpecificLegByUuid = function(uuid, callback)
{
    try
    {
        dbModel.CallCDR.find({where :[{Uuid: uuid}]}).then(function(callLeg)
        {
            if(callLeg)
            {
                callback(null, callLeg);
            }
            else
            {
                callback(null, null);
            }

        });

    }
    catch(ex)
    {
        callback(ex, null);
    }
};

var GetBLegsForIVRCalls = function(uuid, callUuid, callback)
{
    try
    {
        dbModel.CallCDR.findAll({where :[{$or:[{CallUuid: callUuid}, {MemberUuid: callUuid}], Direction: 'outbound', Uuid: {ne: uuid}}]}).then(function(callLegs)
        {
            if(callLegs.length > 0)
            {
                callback(null, callLegs);
            }
            else
            {
                callback(null, callLegs);
            }

        });

    }
    catch(ex)
    {
        var emptyArr = [];
        callback(ex, emptyArr);
    }
};

var publishAbandonCallToQueue = function(obj)
{
    logger.debug('[DVP-CDREngine.publishAbandonCallToQueue] - %s', JSON.stringify(obj));
    logger.debug('[DVP-CDREngine.publishAbandonCallToQueue] - SendAbandonCallsToQueue : %s', JSON.stringify(obj), config.SendAbandonCallsToQueue);
    if(config.SendAbandonCallsToQueue && (config.SendAbandonCallsToQueue === true || config.SendAbandonCallsToQueue === 'true'))
    {
        //CHeck for abandon call
        logger.debug('[DVP-CDREngine.publishAbandonCallToQueue] - CHECK ABANDON');
        if(obj.ObjType === 'HTTAPI' && obj.DVPCallDirection === 'inbound' && obj.IsQueued === true && obj.AgentAnswered === false)
        {
            logger.debug('[DVP-CDREngine.publishAbandonCallToQueue] - IS ABANDON CALL PUBLISH TO QUEUE');
            amqpPublisher('ABANDONED_CALLS', obj)
        }
    }

};

var AddProcessedCDR = function(cdrObj, callback)
{
    try
    {
        var ttl = 5000;
        var lockKey = 'CDRUUIDLOCK:' + cdrObj.Uuid;

        redlock.lock(lockKey, ttl).then(function(lock)
        {
            try
            {
                dbModel.CallCDRProcessed.find({where :[{Uuid: cdrObj.Uuid}]}).then(function(processedCdr)
                {
                    if(!processedCdr)
                    {
                        publishAbandonCallToQueue(cdrObj);

                        console.log('================ SAVING CDR =================');
                        var cdr = dbModel.CallCDRProcessed.build(cdrObj);

                        cdr
                            .save()
                            .then(function (rsp)
                            {
                                lock.unlock()
                                    .catch(function(err) {
                                        logger.error('[DVP-Common.addClusterToCache] - [%s] - REDIS LOCK RELEASE FAILED', err);
                                    });
                                callback(null, true);

                            }).catch(function(err)
                        {
                            lock.unlock()
                                .catch(function(err) {
                                    logger.error('[DVP-Common.addClusterToCache] - [%s] - REDIS LOCK RELEASE FAILED', err);
                                });
                            callback(err, false);
                        })
                    }
                    else
                    {
                        console.log('================ UPDATING CDR =================');
                        processedCdr.updateAttributes(cdrObj).then(function (resUpdate)
                        {

                            callback(null, true);
                            lock.unlock()
                                .catch(function(err) {
                                    logger.error('[DVP-Common.addClusterToCache] - [%s] - REDIS LOCK RELEASE FAILED', err);
                                });

                        }).catch(function (err)
                        {

                            callback(err, false);
                            lock.unlock()
                                .catch(function(err) {
                                    logger.error('[DVP-Common.addClusterToCache] - [%s] - REDIS LOCK RELEASE FAILED', err);
                                });

                        });

                    }

                });

            }
            catch(dbOpEx)
            {
                lock.unlock()
                    .catch(function(err) {
                        logger.error('[DVP-Common.addClusterToCache] - [%s] - REDIS LOCK RELEASE FAILED', err);
                    });
                callback(dbOpEx, false);
            }

        });
    }
    catch(ex)
    {
        callback(ex, false);
    }
};

module.exports.GetSpecificLegsByUuids = GetSpecificLegsByUuids;
module.exports.GetSpecificLegByUuid = GetSpecificLegByUuid;
module.exports.GetBLegsForIVRCalls = GetBLegsForIVRCalls;
module.exports.AddProcessedCDR = AddProcessedCDR;

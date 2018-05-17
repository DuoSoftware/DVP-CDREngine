var dbModel = require('dvp-dbmodels');

var GetSpecificLegsByUuids = function(uuidList, callback)
{
    try
    {
        var query = {where :[{$or: []}]};

        var tempUuidList = [];

        uuidList.forEach(function(uuid)
        {
            tempUuidList.push(uuid);
            query.where[0].$or.push({Uuid: uuid})
        });

        dbModel.CallCDR.findAll(query).then(function(callLegs)
        {
            if(callLegs.length > 0)
            {
                callLegs.forEach(function(callLeg)
                {
                    var index = tempUuidList.indexOf(callLeg.Uuid);
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
                callback(true, callLegs, null);
            }
            else
            {
                var missingLeg = {
                    CallUuid: callUuid,
                    Uuid: uuid
                };
                callback(false, callLegs, missingLeg);
            }

        });

    }
    catch(ex)
    {
        var emptyArr = [];
        callback(false, emptyArr, null);
    }
};

var AddProcessedCDR = function(cdrObj, callback)
{
    try
    {
        dbModel.CallCDRProcessed.find({where :[{Uuid: cdrObj.Uuid}]}).then(function(processedCdr)
        {
            if(!processedCdr)
            {
                var cdr = dbModel.CallCDRProcessed.build(cdrObj);

                cdr
                    .save()
                    .then(function (rsp)
                    {
                        callback(null, true);

                    }).catch(function(err)
                {
                    callback(err, false);
                })
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

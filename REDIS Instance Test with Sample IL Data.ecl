IMPORT redisServer FROM lib_redis;

STRING myRedisServer := '127.0.0.1';

STRING myRedisPort := '6379'; 
  
myRedis := redisServer('--SERVER=' + myRedisServer + ':' + myRedisPort);

UNSIGNED myDatabase := 0;

R1 := RECORD
  
STRING transaction_id;
STRING10 date_added;
STRING context;
STRING request;
STRING response;

END;

DS := DATASET('~test::data::ilsample', R1, THOR);

STRING GenericizeContext(STRING OriginalContext) := FUNCTION

STRING RemovedTransactionId := REGEXREPLACE('<TransactionId>[A-Z0-9]*</TransactionId>', OriginalContext, '<TransactionId>GENERIC</TransactionId>');
STRING RemovedAccountName   := REGEXREPLACE('<Account><Name>.[A-Za-z ]*</Name><Number>', RemovedTransactionId, '<Account><Name>GENERIC</Name><Number>');
STRING RemovedAccountNumber := REGEXREPLACE('</Name><Number>[0-9]*</Number><Type>', RemovedAccountName, '</Name><Number>GENERIC</Number><Type>'); 
STRING RemovedCompanyNumber := REGEXREPLACE('<Company><Number>[0-9]*</Number><Type>', RemovedAccountNumber, '<Company><Number>GENERIC</Number><Type>');

STRING GenericContext := RemovedCompanyNumber;

RETURN GenericContext;

END;

STRING GenericizeRequest(STRING OriginalRequest) := FUNCTION

STRING RemovedUserAccountNumber := REGEXREPLACE('<User><AccountNumber>[0-9]*</AccountNumber></User>', OriginalRequest, '<User><AccountNumber>GENERIC</AccountNumber></User>');
STRING RemovedReferenceNumber   := REGEXREPLACE('<ReferenceNumber>[0-9A-Za-z ]*</ReferenceNumber>', RemovedUserAccountNumber, '<ReferenceNumber>GENERIC</ReferenceNumber>');
STRING RemovedRequestors        := REGEXREPLACE('<Requestors>.*</Requestors>', RemovedReferenceNumber, '<Requestors>GENERIC</Requestors>');

STRING GenericRequest := RemovedRequestors;

RETURN GenericRequest;

END;

STRING IndividualizeResponse(STRING Response, STRING TransactionID, STRING AccountNumber, STRING ReferenceNumber, STRING TrackingNumber) := FUNCTION

STRING InsertedTransactionId   := REGEXREPLACE('<Header><TransactionId>[A-Z0-9]*</TransactionId>', Response, '<Header><TransactionId>' + TransactionID + '</TransactionId>');
STRING InsertedAccountNumber   := REGEXREPLACE('<User><AccountNumber>[0-9]*</AccountNumber>', InsertedTransactionId, '<User><AccountNumber>' + AccountNumber + '</AccountNumber>');
STRING InsertedReferenceNumber := REGEXREPLACE('<ReferenceNumber>.*</ReferenceNumber>', InsertedAccountNumber, '<ReferenceNumber>' + ReferenceNumber + '</ReferenceNumber>'); 
STRING InsertedTrackingNumber  := REGEXREPLACE('<TrackingNumber>[A-Z0-9]*</TrackingNumber>',InsertedReferenceNumber, '<TrackingNumber>' + TrackingNumber + '</TrackingNumber>');

STRING IndividualizedResponse := InsertedTrackingNumber;

RETURN IndividualizedResponse;

END;

R2 := RECORD

STRING thekey;
STRING theresponse;
STRING RequestTransactionId;
STRING ResponseTransactionId;
STRING1 isfromcache;

END;

R2 XF(DS L) := TRANSFORM

STRING myTransactionID   := REGEXFIND('<Common><TransactionId>([A-Z0-9]*)</TransactionId>', L.context, 1);
STRING myAccountNumber   := REGEXFIND('</Name><Number>([0-9]*)</Number><Type>', L.context, 1);
STRING myReferenceNumber := REGEXFIND('<ReferenceNumber>([0-9A-Za-z ]*)</ReferenceNumber>', L.request, 1);
STRING myTrackingNumber  := myTransactionID;

STRING GenericContext := GenericizeContext(L.context);
STRING GenericRequest := GenericizeRequest(L.request);

STRING myHashKey := (STRING) HASH64(GenericContext) + (STRING) HASH64(GenericRequest);

STRING myCachedValue := myRedis.GetOrLockString(myHashKey, myDatabase);
STRING myPublishedValue := myRedis.SetAndPublishString(myHashKey, L.response, myDatabase);

SELF.thekey := myHashKey;

STRING myResponse := IF(LENGTH(myCachedValue) = 0, myPublishedValue, myCachedValue);

SELF.RequestTransactionId := myTransactionID;
SELF.ResponseTransactionId := REGEXFIND('<Header><TransactionId>([A-Z0-9]*)</TransactionId>', myResponse, 1);

SELF.theresponse := IndividualizeResponse(myResponse, myTransactionId, myAccountNumber, myReferenceNumber, myTrackingNumber);

SELF.isFromCache := IF(LENGTH(myCachedValue) = 0, 'N', 'Y');
 	
END;

KV := PROJECT(DS, XF(LEFT));

R3 := RECORD

UNSIGNED volume := COUNT(GROUP);
UNSIGNED min_key_len := MIN(GROUP, LENGTH(KV.thekey));
UNSIGNED avg_key_len := AVE(GROUP, LENGTH(KV.thekey));
UNSIGNED max_key_len := MAX(GROUP, LENGTH(KV.thekey));
UNSIGNED hits := COUNT(GROUP, KV.isfromcache = 'Y');
UNSIGNED misses := COUNT(GROUP, KV.isfromcache = 'N');
DECIMAL5_2 hit_rate := (COUNT(GROUP, KV.isfromcache = 'Y') / COUNT(GROUP)) * 100;
DECIMAL5_2 miss_rate := (COUNT(GROUP, KV.isfromcache = 'N') / COUNT(GROUP)) * 100;
UNSIGNED min_rsp_len := MIN(GROUP, LENGTH(KV.theresponse));
UNSIGNED avg_rsp_len := AVE(GROUP, LENGTH(KV.theresponse));
UNSIGNED max_rsp_len := MAX(GROUP, LENGTH(KV.theresponse));

END;

T := TABLE(KV, R3);

OUTPUT(T, NAMED('Statistics'));


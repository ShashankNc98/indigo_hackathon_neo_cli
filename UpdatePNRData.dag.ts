import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getOut } = dao;

@Dag({ method: "POST", url: "v1/mongo-insert" })
class UpdatePNRData {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: 40, y: -66 } })
  @Relation(r => dao.isSuccess(), 'changePathBasedOnCollectionName')
  async AppConfigurations() {
    const script = {

        execute: () => {

            const appVersion = "1.4.0";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            const developer="Adarsh"
                const branch="PSV-29061"   
                const trigger = "/v1/mongo-insert" 
                const requestBody = this.dao.getApiRequest()?.body?.[0];
                const externalId = requestBody?.FFN
                const billNumber = requestBody?.billNumber
                const isgRequestId = `${trigger}_${externalId}_${billNumber}`;
                logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);

            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 522, y: -66 } })
  @Relation(r => dao.isSuccess(), 'PutMongo')
  async CreateQueryForPutMongo() {
    const script = {

        execute: () => {
            const requestBody = getApiRequest("Trigger")?.body;
            const filterRequestBody = [];
            const queryArray = [];
            let mongoQuery = {};
            let mongoKey = {};

            for (let data of requestBody) {
                if (data.PNR_KEY) {
                    filterRequestBody.push(data);
                }
            }

            if (filterRequestBody?.length > 0) {
                for (let data of filterRequestBody) {
                    let customFieldsObject = {};
                    for (const key in data) {
                        const match = key.match(/^customFields_(CF\d+)$/);
                        if (match) {
                            const newKey = match[1];
                            customFieldsObject[newKey] = data[key];
                        }
                    }

                    // Call processDateTime for arrivalDate and departureDate
                    const arrivalObj = processDateTime(data.arrivalDate);
                    const departureObj = processDateTime(data.departureDate);

                    mongoKey = {
                        "PNR_KEY": (data?.PNR_KEY)?.trim().replace(/\s+/g, '')
                    };
                    mongoQuery = {
                        $set: {
                            "PNR": data?.PNR,
                            "PNR_KEY": (data?.PNR_KEY)?.trim().replace(/\s+/g, ''),
                            "billNumber": data?.billNumber,
                            "ticketNumber": data?.ticketNumber,
                            "FFN": data?.FFN,
                            "sourceStore": data?.sourceStore,
                            "origin": data?.origin,
                            "destination": data?.destination,
                            "distanceTravelled": data?.distanceTravelled,
                            "departureDate": departureObj.date,  // only date
                            "firstName": (data?.firstName)?.trim().replace(/\s+/g, ''),
                            "lastName": (data?.lastName)?.trim().replace(/\s+/g, ''),
                            "source": data?.source,
                            "deviceId": data?.deviceId,
                            "passengerid": data?.passengerid,
                            "bookingid": data?.bookingid,
                            "eticket": data?.eticket,
                            "arrivalDate": arrivalObj.date,      // only date
                            "customFields": customFieldsObject,
                            "arrivalTime": arrivalObj.time,      // time only
                            "departureTime": departureObj.time,  // time only
                            "isActive": true,
                            "modifiedDate": new Date()
                        },
                        $setOnInsert: {
                            "creationDate": new Date()
                        }
                    };
                    queryArray.push({
                        mongoKey,
                        mongoQuery
                    });

                }
                return queryArray;

            }

            return {
                http: {
                    res: {
                        status: 200,
                        "json": "PNR_KEY field not present or requestBody is an empty array",
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }

    // Converts datetime string to separate date and time
    function processDateTime(datetimeStr) {
        if (!datetimeStr) return { date: null, time: null };
        const d = new Date(datetimeStr);
        if (isNaN(d)) return { date: null, time: null };

        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, "0");
        const dd = String(d.getDate()).padStart(2, "0");

        const hh = String(d.getHours()).padStart(2, "0");
        const min = String(d.getMinutes()).padStart(2, "0");
        const ss = String(d.getSeconds()).padStart(2, "0");

        return {
            date: `${yyyy}-${mm}-${dd}`,
            time: `${hh}:${min}:${ss}`
        };
    }
  }

  @PutMongo({ pos: { x: 782, y: -70 } })
  @Relation(r => dao.isSuccess(), 'FinalResponse')
  async PutMongo() {
  return {
        collectionName: `UtilisedPNR`,
        mode: `upsert`,
        query: r => getBody().mongoQuery,
        queryKey: r => getBody().mongoKey,
      };
  }

  @Script({ pos: { x: 1102, y: -70 } })
  async FinalResponse() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http:{
                    res:{
                        status:200,
                        "json":getOut("PutMongo"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 271, y: -105 } })
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.collectionName === "UtilisedPNR"), 'CreateQueryForPutMongo')
  @Relation(r => dao.isSuccess() && (dao.getBody()?.body?.collectionName === "nomineeProcessingLocks"), 'createPayloadForNomineeDetailsCollection')
  async changePathBasedOnCollectionName() {
    const script = {

        execute: () => {

            const apiRequest = getApiRequest("Trigger");
            const headers = apiRequest?.headers || {};

            // Case-insensitive check
            const collectionName =
                Object.keys(headers).find(
                    key => key.toLowerCase() === "collectionname"
                );

            const finalCollection =
                collectionName ? headers[collectionName] : null;

            logger.info(`Collection Name From Header: ${finalCollection}`);

            return {
                body: {
                    collectionName: finalCollection || "UtilisedPNR"
                }
            };
        }
    };
  }

  @Script({ pos: { x: 513, y: 107 } })
  @Relation(r => dao.isSuccess(), 'UpsertNominee')
  async createPayloadForNomineeDetailsCollection() {
    const script = {

        execute: () => {

            const headers = getApiRequest()?.headers;
            const collectionName = headers?.collectionname || "utilisedPnr";

            // ✅ Validate collection
            if (collectionName !== "nomineeProcessingLocks") {
                return {
                    http: {
                        res: {
                            status: 400,
                            json: {
                                status: false,
                                code: 400,
                                message: "Invalid collectionName. Expected nomineeProcessingLocks"
                            }
                        }
                    }
                };
            }

            const requestBody = getApiRequest()?.body;

            // ✅ Validate required fields
            if (!requestBody?.identifierType || !requestBody?.identifierValue) {
                return {
                    http: {
                        res: {
                            status: 400,
                            json: {
                                status: false,
                                code: 400,
                                message: "identifierType and identifierValue are mandatory"
                            }
                        }
                    }
                };
            }

            // ✅ Plain document for INSERT
            const document = {
                identifierType: requestBody.identifierType,
                identifierValue: requestBody.identifierValue,
                isActive: true,
                expiresAt: { "$date": new Date().toISOString() },
                createdDate: new Date(),
                modifiedDate: new Date()
            };

            // For insertMongo block, just return array of documents
            return [document];
        }
    };
  }

  @PutMongo({ pos: { x: 774, y: 104 } })
  @Relation(r => dao.isSuccess(), 'finalResponseOfNominee')
  @Relation(r => dao.hasError(), 'errorMongo')
  async UpsertNominee() {
  return {
        collectionName: `nomineeProcessingLocks`,
        mode: `insert`,
        query: r => getBody(),
        queryKey: ``,
        options: ``,
      };
  }

  @Script({ pos: { x: 1091.5, y: 77 } })
  async finalResponseOfNominee() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http:{
                    res:{
                        status:200,
                        "json":getOut("UpsertNominee"),
                        "headers": {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1094, y: 264 } })
  async errorMongo() {
    const script = {
      execute: () => {

        const res = getOut()?.[0];

        // 🔴 Handle Duplicate Key Error (Mongo 11000)
        if (res?.err?.code === 11000) {

          logger.error("Duplicate key error detected", res);

          return {
            http: {
              res: {
                status: 200,   // keeping 200 as per your pattern
                json: {
                  status: false,
                  code: 409,
                  message: "Identifier already exists",
                  error: "Duplicate identifierValue"
                },
                headers: {
                  "App-Version":
                    getBody("AppConfigurations")?.APP_VERSION
                }
              }
            }
          };
        }
        return res;
      }
    };
  }
}

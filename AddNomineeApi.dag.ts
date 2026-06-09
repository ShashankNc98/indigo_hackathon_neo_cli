import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getError, getOut } = dao;

@Dag({ method: "POST", url: "addnominee" })
class AddNomineeApi {
  constructor() {
    this.AppConfigurations();
  }

  @Schema({ pos: { x: 81.9493263034563, y: 38.37691857059173 } })
  @Relation(r => dao.hasError(), 'ResponseForInvalidRequests')
  @Relation(r => dao.isSuccess(), 'CheckNomineeSpec')
  async RequestValidator() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          body: {
            type: 'object',
            properties: {
              identifierType: {
                type: 'string',
                minLength: 1,
                transform: ['trim', 'toLowerCase'],
                errorMessage: {
                  minLength: "identifierType cannot be empty"
                }
              },
              identifierValue: {
                type: 'string',
                minLength: 1,
                transform: ['trim', 'toLowerCase'],
                errorMessage: {
                  minLength: "identifierValue cannot be empty"
                }
              },
              Nominee: {
                type: 'array',
                minItems: 1,
                items: {
                  type: 'object',
                  properties: {
                    Gender: {
                      type: 'string',
                      minLength: 1,
                      transform: ['trim', 'toLowerCase'],
                      enum: ['male', 'female', 'other'],
                      "errorMessage": {
                        minLength: "Gender must not be empty",
                        enum: "Invalid Gender.Gender must be 'Male/Female/Other'"
                      }
                    },
                    FirstName: {
                      type: 'string',
                      transform: ['trim'],
                      minLength: 1,
                      "errorMessage": {
                        minLength: "FirstName must not be empty"
                      }
                    },
                    LastName: {
                      type: 'string',
                      transform: ['trim'],
                      minLength: 1,
                      "errorMessage": {
                        minLength: "LastName must not be empty"
                      }
                    },
                    DOB: {
                      type: 'string',
                      transform: ['trim'],
                      format: "date",
                      minLength: 1,
                      "errorMessage": {
                        minLength: "DOB must not be empty",
                        format: "DOB date format must be YYYY-MM-DD"
                      }
                    },
                  },
                  required: ['Gender', 'FirstName', 'LastName', 'DOB'],
                  errorMessage: {
                    required: {
                      Gender: "gender is missing",
                      FirstName: "FirstName is missing",
                      LastName: "LastName is missing",
                      DOB: "dob is missing",
                    }
                  }
                }
              }
            },
            required: ['identifierType', 'identifierValue', 'Nominee'],
            errorMessage: {
              required: {
                identifierType: "identifierType is missing",
                identifierValue: "identifierValue is missing",
                Nominee: "nominee details are missing"
              }
            }
          }
        },
        required: ['body'],
        errorMessage: {
          required: {
            body: "Payload is missing"
          }
        }
      }
    }
  }

  @Script({ pos: { x: 344.17311072056225, y: 238.00591681312244 } })
  @Relation(r => dao.isSuccess(), 'createPayloadForNomineeLocksCollection')
  async CheckNomineeSpec() {
    const script = {

      execute: () => {
        const requestIdentifierType = getApiRequest().body.identifierType
        const requestIdentifierValue = getApiRequest().body.identifierValue
        const mongoQuery = {
          identifierType : requestIdentifierType,
          identifierValue: requestIdentifierValue,
          isActive : true
        };

         return {
          body: {
            query : JSON.stringify(mongoQuery),
          }
        };

      }
    }
  }

  @GetMongo({ pos: { x: 1037.3545401288811, y: 22.255594610427636 } })
  @Relation(r => dao.isSuccess(), 'NomineeCountValidation')
  async GetNomineeDetails() {
  return {
        collectionName: `NomineeDetails`,
        query: r => getBody("CheckNomineeSpec").body.query,
        sort: `{}`,
        limit: 10,
      };
  }

  @Script({ pos: { x: 1299, y: 38 } })
  @Relation(r => dao.isSuccess() && dao.getBody()?.body?.isAddNomineeCaseFound, 'MongoPutSpecForAddNominee')
  async NomineeCountValidation() {
    const script = {
      execute: () => {

        const maximumAllowedNomineeCount = 5;
        const nomineeDbDetails = getOut("GetNomineeDetails");
        const nomineeDbCount = nomineeDbDetails?.length;
        console.log("getApiRequest:" + getApiRequest());
        const requestNomineeCount = getApiRequest()?.body?.Nominee?.length;

        let isAddNomineeCaseFound = false;

        if (requestNomineeCount > maximumAllowedNomineeCount) {
          return {
            http: {
              "res": {
                "json": {
                  availableNomineeCount: nomineeDbCount,
                  requestedNomineecount: requestNomineeCount,
                  errors: [
                    {
                      success: false,
                      code: 1001,
                      message: "input request nominee count exceeds the max allowed limit"
                    }
                  ]
                },
                "status": 200,
                headers: {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };

        }

        else if (nomineeDbCount === maximumAllowedNomineeCount) {
          // If Db Nominee count is already 5 available then return the api response [max limit reached”]

          return {
            http: {
              "res": {
                "json": {
                  availableNomineeCount: nomineeDbCount,
                  requestedNomineecount: requestNomineeCount,
                  isAddNomineeCaseFound,
                  errors: [
                    {
                      success: false,
                      code: 1002,
                      message: "max limit reached for nominee"
                    }
                  ]
                },
                "status": 200,
                headers: {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };



        }
        else if (sumCalculation(requestNomineeCount, nomineeDbCount) > maximumAllowedNomineeCount) {
          return {
            http: {
              "res": {
                "json": {
                  availableNomineeCount: nomineeDbCount,
                  requestedNomineecount: requestNomineeCount,
                  isAddNomineeCaseFound,
                  errors: [
                    {
                      success: false,
                      code: 1003,
                      message: "incorrect nominee count request received"
                    }
                  ]
                },
                "status": 200,
                headers: {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };


        }

        else {
          isAddNomineeCaseFound = true;
          return {
            body: {
              status: 200,
              isAddNomineeCaseFound,
              availableNomineeCount: nomineeDbCount,
              requestedNomineecount: requestNomineeCount,
              "message": "Add Nominees Action Will Trigger Now",
              "messageCode": "1000",
            }
          }
        }
      }

    }

    function sumCalculation(inputNomineeCount, dbNomineeCount) {
      return inputNomineeCount + dbNomineeCount;
    }
  }

  @Script({ pos: { x: 1588.8940625, y: 40.055468750000045 } })
  @Relation(r => dao.isSuccess(), 'AddNomineesToDb')
  async MongoPutSpecForAddNominee() {
    const script = {

      execute: () => {
        let currentDate = new Date();
        let requestBodyData = getApiRequest().body;
        let addNomineesPutMongo = [];

        if (requestBodyData && requestBodyData.Nominee && Array.isArray(requestBodyData.Nominee)) {
            requestBodyData.Nominee.forEach(nominee => {      
                let nomineeRecordForQuery = {
                    identifierType: requestBodyData.identifierType,
                    identifierValue: requestBodyData.identifierValue,
                    isActive : true,
                    gender : nominee.Gender,
                    firstName : nominee.FirstName,
                    lastName : nominee.LastName,
                    dob : nominee.DOB,
                    IsMinor: nominee?.IsMinor,
                    ConsentDetails: nominee?.ConsentDetails,
                    createdDate: currentDate,
                    modifiedDate: currentDate
                };
                addNomineesPutMongo.push(nomineeRecordForQuery);
          });
        }


        return {
                status : 200,
                body : {
                    "addNomineesPutMongoQuery" : JSON.stringify(addNomineesPutMongo)
                }
            }

      }
    }
  }

  @PutMongo({ pos: { x: 1900, y: 0 } })
  @Relation(r => dao.isSuccess(), 'FetchNomineDetails')
  async AddNomineesToDb() {
  return {
        collectionName: `NomineeDetails`,
        mode: `insert`,
        query: r => getBody("MongoPutSpecForAddNominee").body.addNomineesPutMongoQuery,
        queryKey: `{}`,
      };
  }

  @GetMongo({ pos: { x: 2184, y: -12 } })
  @Cachable({ cachable: false })
  @Relation(r => dao.isSuccess(), 'ResponseHandlingPostDBFetch')
  async FetchNomineDetails() {
  return {
        collectionName: `NomineeDetails`,
        query: r => getBody("CheckNomineeSpec").body.query,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 2504, y: -12 } })
  async ResponseHandlingPostDBFetch() {
    const script = {
      execute: () => {

        let nomineeList = getOut("FetchNomineDetails")?.map((ele) => {
          let { gender, firstName, lastName, dob, createdDate } = ele;
          return {
            nomineeId: Object.values(ele._id.buffer).map(byte => byte.toString(16).padStart(2, '0')).join(''),
            gender: gender ? gender : "",
            firstName: firstName ? firstName : "",
            lastName: lastName ? lastName : "",
            dob: dob ? dob : "",
            createdDate: createdDate ? createdDate : ""
          };
        });
        if (nomineeList?.length > 0) {
          return {
            http: {
              "res": {
                "json": {
                  messageCode: "200",
                  message: "Successful",
                  identifierType: getApiRequest().body.identifierType,
                  identifierValue: getApiRequest().body.identifierValue,
                  nominee: nomineeList
                },
                "status": 200, //http response code ( number )
                headers: {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };
        } else {

          return {
            http: {
              "res": {
                "json": {
                  identifierType: getApiRequest().body.identifierType,
                  identifierValue: getApiRequest().body.identifierValue,
                  errors: [
                    {
                      success: false,
                      code: 1007,
                      message: "No nominee available now.Please try again later"
                    }
                  ]
                },
                "status": 200, //http response code ( number )
                headers: {
                  "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                }
              }
            }
          };

        }
      }
    }
  }

  @Script({ pos: { x: 316.02176699716813, y: -148.49627011431875 } })
  async ResponseForInvalidRequests() {
    const script = {
        execute: () => {
            //logger.info("hello ResponseForInvalidRequests", JSON.stringify(dao.getError("RequestValidator")))
            //logger.info('hello ResponseForInvalidRequests', JSON.stringify(dao.getOut("RequestValidator")));

            const errorArray = [];
            const validationErrors = getError("RequestValidator")?.err;
            validationErrors?.forEach(validationError => {
                const error = {
                    status: false,
                    "message": validationError.message,
                    "code": 400,
                    "path": validationError.instancePath
                }
                errorArray.push(error)
            });

            return {
                http: {
                    "res": {
                        "json": {
                            errors: errorArray
                        }, //any
                        "status": 400, //http response code ( number )
                        headers: {
                            "App-Version": getBody("AppConfigurations")?.body.APP_VERSION
                        }
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: -164.1531311786029, y: 53.51177656593677 } })
  @Relation(r => dao.isSuccess(), 'RequestValidator')
  async AppConfigurations() {
    const script = {

        execute: () => {
            const appVersion = "1.2";
            logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`)
            let developer = "Adarsh"
            let ticket = "PSV-29061"
            return {
                body:
                {
                    APP_VERSION: appVersion
                }
            };

        }
    }
  }

  @Script({ pos: { x: 541.4792911540715, y: 55.095215850866396 } })
  @Relation(r => dao.isSuccess(), 'UpsertNomineeLocks')
  async createPayloadForNomineeLocksCollection() {
    const script = {

        execute: () => {

            const requestBody = getApiRequest()?.body;


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

  @PutMongo({ pos: { x: 782.2072349150557, y: 83.00570208402401 } })
  @Relation(r => dao.isSuccess(), 'GetNomineeDetails')
  @Relation(r => dao.hasError(), 'errorNomineeLocks')
  async UpsertNomineeLocks() {
  return {
        collectionName: `nomineeProcessingLocks`,
        mode: `insert`,
        query: r => getBody(),
        queryKey: ``,
        options: ``,
      };
  }

  @Script({ pos: { x: 1059.5676918570593, y: 252.2130248725419 } })
  async errorNomineeLocks() {
    const script = {
      execute: () => {

        const res = getOut()?.[0];

        // Handle Duplicate Key Error (Mongo 11000)
        if (res?.err?.code === 11000) {

          logger.error("Duplicate key error detected", res);

          return {
            http: {
              res: {
                status: 200,
                json: {
                  errors: [
                    {
                      status: false,
                      message: "Identifier already exists",
                      code: 400,
                      path: "/body"
                    }
                  ]
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

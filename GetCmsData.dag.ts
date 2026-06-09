import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getIn, getOut } = dao;

@Dag({ method: "GET", url: "cms" })
class GetCmsData {
  constructor() {
    this.SchemaValidation();
  }

  @Schema({ pos: { x: 266, y: 89 } })
  @Relation(r => dao.isSuccess(), 'PrepareMongoFetchQuery')
  @Relation(r => dao.hasError(), 'queryParamValidationFailureBlock')
  async SchemaValidation() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          queryParams: {
            type: 'object',
            properties: {
              connectPlusDataflowId: {
                type: 'string',
                minLength: 1,
                transform: ['trim', 'toLowerCase'],
                errorMessage: {
                  minLength: "connectPlusDataflowId cannot be empty"
                }
              },
              partner: {
                type: 'string',
                minLength: 1,
                transform: ['trim', 'toLowerCase'],
                errorMessage: {
                  minLength: "partner cannot be empty"
                }
              }
            },
            required: ['connectPlusDataflowId', 'partner'],
            errorMessage: {
              required: {
                connectPlusDataflowId: "connectPlusDataflowId is missing",
                partner: "partner is missing"
              }
            }
          }
        },
        required: ['queryParams'],
        errorMessage: {
          required: {
            queryParams: "queryParams are missing"
          }
        }
      }
    }
  }

  @Script({ pos: { x: 620, y: 0 } })
  @Relation(r => dao.isSuccess(), 'FetchCmsData')
  async PrepareMongoFetchQuery() {
    const script = {

        execute: () => {
            const queryParams = getApiRequest().queryParams;

            const connectPlusDataflowId = queryParams.connectPlusDataflowId;
            const partner = queryParams.partner;

            const query = {connectPlusDataflowId, partner};
            return {
                body: {
                    query
                }
            };

        }
    }
  }

  @Script({ pos: { x: 627, y: 244 } })
  async queryParamValidationFailureBlock() {
    const script = {

        execute: () => {
            let errorArr = [];
            let error;
            const validationErrors = getIn()?.err;
            validationErrors?.forEach(validationError => {
                error = {
                    "status": false,
                    "code": 6001,
                    "message" : validationError.message,
                    "path" : validationError.instancePath
                }
                errorArr.push(error);
            });
            return {
               http: {
                   "res": {
                        status : 400,
                        "json": {
                            "errors" : errorArr
                        }
                   }
               }
            }
        }

    }
  }

  @GetMongo({ pos: { x: 940, y: 0 } })
  @Relation(r => dao.isSuccess(), 'mongoResponse')
  async FetchCmsData() {
  return {
        collectionName: `CMS_Schema`,
        query: r => getBody().body.query,
        sort: `{}`,
        options: `{ "projection": { "_id": 0 } }`,
      };
  }

  @Script({ pos: { x: 1260, y: 0 } })
  @Relation(r => dao.isSuccess(), 'entitiesApiPayload')
  async mongoResponse() {
    const script = {

        execute: () => {
            const cmsDataFromMongo = getOut("FetchCmsData");

            if (cmsDataFromMongo.length == 0) {
                return {
                    http: {
                        res: {
                            json: {
                                errors: [
                                    {
                                        status: false,
                                        code: 9000,
                                        message: "No data found for given partner and connectPlusDataflowId"
                                    }
                                ]
                            },
                            status: 200
                        }
                    }
                }
            }

            return {
                data: cmsDataFromMongo
            };

        }
    }
  }

  @Script({ pos: { x: 1580, y: 0 } })
  @Relation(r => dao.isSuccess(), 'entitiesApiCall')
  async entitiesApiPayload() {
    const script = {

        execute: () => {

            //Write your code here.
            const cmsData = getBody("mongoResponse").data;
            const storeId = cmsData[0].storeId;

            if (!storeId) {
                logger.info("Store id not found error: storeId not found. So returning cms data from mongo");
                return {
                    http: {
                        res: {
                            json: cmsData[0],
                            status: 200
                        }
                    }
                }
            }

            const queryParams = {
                type: "STORE",
                "include_parent": true,
                id: storeId
            }
            const requestHeaders = getApiRequest().headers;

            const headers = requestHeaders["x-cap-api-oauth-token"];

            return {
                queryParams,
                headers    
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 1900, y: 0 } })
  @Cachable({ cachable: true, key: r => dao.getBody("entitiesApiPayload").queryParams.id, ttl: 600 })
  @Relation(r => dao.isSuccess(), 'finalResponse')
  @Relation(r => dao.hasError(), 'entitiesApiCallFail')
  async entitiesApiCall() {
  return {
        url: `https://apac.api.capillarytech.com/v1.1/organization/entities`,
        method: `GET`,
      };
  }

  @Script({ pos: { x: 2219, y: -63 } })
  async finalResponse() {
    const script = {

        execute: () => {

            //Write your code here.
            const cmsData = getBody("mongoResponse").data[0];
            const entityData = getBody("entitiesApiCall").response;

            const conceptCode = entityData?.organization?.entities?.entity[0]?.["parent_concept_code"];
            const zoneCode = entityData?.organization?.entities?.entity[0]?.["parent_code"];
            const storeCode = entityData?.organization?.entities?.entity[0]?.["code"];
            const parentConceptName = entityData?.organization?.entities?.entity[0]?.["parent_concept_name"];
            const name = entityData?.organization?.entities?.entity[0]?.["name"];

            cmsData["conceptCode"] = conceptCode;
            cmsData["zoneCode"] = zoneCode;
            cmsData["storeCode"] = storeCode;
            cmsData["parentConceptName"] = parentConceptName;
            cmsData["name"] = name;

            return {
                http: {
                    res: {
                        json: cmsData,
                        status: 200
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 2220, y: 160 } })
  async entitiesApiCallFail() {
    const script = {

        execute: () => {

            //Write your code here.
            return {
                http: {
                    res: {
                        json: {
                            errors: [
                                {
                                    status: false,
                                    code: 9002,
                                    message: "Error occured while invoking Entities api"
                                }
                            ]
                        },
                        status: 200
                    }
                }
            };

        }
    }
  }
}

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getError, getOut, getValueByKey } = dao;

@Dag({ method: "PUT", url: "updateNominee" })
class UpdateNomineeApi {
  constructor() {
    this.versionConfig();
  }

  @Script({ pos: { x: 989.1687124430337, y: -170.80271716308596 } })
  @Relation(r => dao.isSuccess(), 'getNomineeData')
  async getPayload() {
    const script = {

        execute: () => {
            const body = getApiRequest().body || {};
            if (!body.nomineeId) {
                return {
                    http: {
                        res: {
                            status: 400,
                            json: {
                                message: "nomineeId is required"
                            }
                        }
                    }
                };
            }
            const requestBodyKeys = Object.keys(body);
            // If only nomineeId is provided
            if (requestBodyKeys.length <= 1) {
                return {
                    http: {
                        res: {
                            status: 400,
                            json: {
                                message: "No data provided for update"
                            }
                        }
                    }
                };
            }
            const mongoQueryKey = {
                _id: { $oid: body.nomineeId },
                isActive: true

            };


            const mongoQueryData = {
                modifiedDate: new Date()
            };

            ["gender", "firstName", "lastName", "dob"].forEach((key) => {
                if (body[key] !== undefined) {
                    mongoQueryData[key] = body[key];
                }
            });

            // Enrichment fields — always updatable, no cooldown
            ["relation", "passport", "avatar"].forEach((key) => {
                if (body[key] !== undefined) {
                    mongoQueryData[key] = body[key];
                }
            });

            // Flag to carry forward whether identity fields are being touched
            const hasIdentityFields = ["gender", "firstName", "lastName", "dob"]
                .some((key) => body[key] !== undefined);

            logger.info(`Building update query | nomineeId: ${body.nomineeId} | fields: ${Object.keys(mongoQueryData).join(", ")} | hasIdentityFields: ${hasIdentityFields}`);


            return {
                body: {
                    queryKey: JSON.stringify(mongoQueryKey),
                    queryData: JSON.stringify({ $set: mongoQueryData }),
                    hasIdentityFields
                }
            };
        }
    };
  }

  @GetMongo({ pos: { x: 1237.2185270996094, y: -149.40815148925776 } })
  @Relation(r => (dao.isSuccess() && !dao.getOut("getNomineeData")?.length == 0), 'validateModifiedDate')
  @Relation(r => (dao.isSuccess() && dao.getOut("getNomineeData")?.length == 0), 'getNomineeFail')
  async getNomineeData() {
  return {
        collectionName: `NomineeDetails`,
        query: r => getBody().body.queryKey,
        sort: `{}`,
      };
  }

  @PutMongo({ pos: { x: 1755.1274638264974, y: -181.6415479329427 } })
  @Relation(r => dao.isSuccess(), 'fetchNomineeDetails')
  async updateDBNomineeData() {
  return {
        collectionName: `NomineeDetails`,
        mode: `update`,
        query: r => getBody("validateModifiedDate").body.queryData,
        queryKey: r => getBody("validateModifiedDate").body.queryKey,
        options: ``,
      };
  }

  @Schema({ pos: { x: 729.7424465332031, y: -65.8932892659505 } })
  @Relation(r => dao.isSuccess(), 'getPayload')
  @Relation(r => dao.hasError(), 'validationFailure')
  async validateRequestSchema() {
    return {
        definitions: [],
        spec: {
            type: "object",
            properties: {
                body: {
                    type: "object",
                    properties: {
                        nomineeId: {
                            type: "string",
                            minLength: 24,
                            maxLength: 24,
                            errorMessage: {
                                minLength: "nomineeId must be of 24 letters",
                                maxLength: "nomineeId must be of 24 letters"
                            }
                        },
                        gender: {
                            type: 'string',
                            minLength: 1,
                            transform: ['trim', 'toLowerCase'],
                            enum: ['male', 'female', 'other'],
                            "errorMessage": {
                                minLength: "gender must not be empty",
                                enum: "Invalid gender. Gender must be 'Male/Female/Other'"
                            }
                        },
                        firstName: {
                            type: 'string',
                            transform: ['trim'],
                            minLength: 1,
                            "errorMessage": {
                                minLength: "firstName must not be empty"
                            }
                        },
                        lastName: {
                            type: 'string',
                            transform: ['trim'],
                            minLength: 1,
                            "errorMessage": {
                                minLength: "lastName must not be empty"
                            }
                        },
                        dob: {
                            type: 'string',
                            transform: ['trim'],
                            format: "date",
                            minLength: 1,
                            "errorMessage": {
                                minLength: "dob must not be empty",
                                format: "dob date format must be YYYY-MM-DD"
                            }
                        }
                    },
                    required: ["nomineeId"],
                    errorMessage: {
                        required: {
                            nomineeId: "nomineeId is missing"
                        }
                    }
                }
            },
            required: ["body"],
            errorMessage: {
                required: {
                    body: "Payload is missing"
                }
            }
        }
    }
  }

  @Script({ pos: { x: 977.5995069580081, y: 156.1293537597656 } })
  async validationFailure() {
    const script = {

        execute: () => {

            //Write your code here.
            const errorsArray = [];
            const validationErrors = getError("validateRequestSchema")?.err;
            validationErrors?.forEach((validationError) => {
                const error = {
                    status: false,
                    code: 400,
                    message: `${validationError.message}`
                };
                errorsArray.push(error);
            });

            return {
                http: {
                    res: {
                        json: {
                            errors: errorsArray
                        },
                        status: 400,
                        headers: getBody("versionConfig").headers
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 246.09625476074217, y: -34.88876066080729 } })
  @Relation(r => dao.isSuccess(), 'StaticConfiguration')
  async versionConfig() {
    const script = {

        execute: () => {
            const developer = "Adarsh"
            const branch = "PSV-29940"
            const trigger = "/updateNominee"
            const requestBody = getApiRequest()?.body;
            const nomineeId = requestBody?.nomineeId
            const firstName = requestBody?.firstName
            const isgRequestId = `${trigger}_${nomineeId}_${firstName}`;
            logger.info(`IsgRequestId : ${JSON.stringify(isgRequestId)}`);
            //Write your code here.
            return {
                headers: {
                    "x-cap-isg-neo-verison": 1.4
                }
            }
        }
    }
  }

  @GetMongo({ pos: { x: 1988.8226518554688, y: -178.57292240397135 } })
  @Relation(r => dao.isSuccess(), 'responseAfterDBUpdation')
  async fetchNomineeDetails() {
  return {
        collectionName: `NomineeDetails`,
        query: r => getBody("getPayload").body.queryKey,
        sort: `{}`,
      };
  }

  @Script({ pos: { x: 2264.6544651448567, y: -193.47178495279945 } })
  async responseAfterDBUpdation() {
    const script = {

        execute: () => {

            const nomineesFromDB = getOut("fetchNomineeDetails") || [];
            logger.info(`Fetch nominee response | count: ${nomineesFromDB.length}`);
            const nomineeList = nomineesFromDB.map((nominee) => ({
                gender: nominee?.gender || "",
                firstName: nominee?.firstName || "",
                lastName: nominee?.lastName || "",
                dob: nominee?.dob || "",
                createdDate: nominee?.createdDate || "",
                modifiedDate: nominee?.modifiedDate || ""   
            }));

            return {
                http: {
                    res: {
                        status: 200,
                        json: {
                            messageCode: "200",
                            message: "Successful",
                            nominee: nomineeList,
                            errors: []
                        },
                        headers: getBody("versionConfig")?.headers
                    }
                }
            };
        }
    };
  }

  @Script({ pos: { x: 1568.3976955159505, y: 132.17011120605468 } })
  async getNomineeFail() {
    const script = {

        execute: () => {
            const errors = [];

            let error = {
                "status": false,
                "code": 9812,
                "message": "No data for this nominee exists"
            };

            errors.push(error);

            return {
                http: {
                    res: {
                        json: {
                            errors
                        },
                        status: 400,
                        headers: getBody("versionConfig").headers
                    }
                }
            };

        }
    }
  }

  @Script({ pos: { x: 1502.2411701253254, y: -187.9954713948568 } })
  @Relation(r => dao.isSuccess(), 'updateDBNomineeData')
  async validateModifiedDate() {
    const buildErrorResponse = (message, code) => ({
      http: {
        res: {
          status: 400,
          json: {
            errors: [
              {
                status: false,
                code: code,
                message
              }
            ]
          },
          headers: getBody("versionConfig")?.headers
        }
      }
    });

    const script = {
      execute: () => {
        const body = getBody("getPayload")?.body;
        const nomineeData = getOut("getNomineeData")?.[0];
        const literals = getBody("StaticConfiguration")?.body;
        const nomineeXAPIKeyFromConfig = literals?.nomineeXApiKey || "";

        // --- tillId bypass check ---
        const requestHeaders = getApiRequest()?.headers || {};
        const apiKeyHeader = requestHeaders?.["x-api-key"] || "";
        logger.info(`validateModifiedDate | headers received: ${JSON.stringify(requestHeaders)}`);

        const parsedQueryKey = JSON.parse(body?.queryKey || "{}");

        if (apiKeyHeader === nomineeXAPIKeyFromConfig) {
          logger.info(`x-api-key match — skipping 365-day validation`);
          const parsedQueryKey = JSON.parse(body?.queryKey || "{}");
          return {
            body: {
              queryKey: JSON.stringify(parsedQueryKey),
              queryData: body?.queryData
            }
          };
        }

        // Enrichment-only update — no identity fields touched, skip 365-day check
        if (!body?.hasIdentityFields) {
          logger.info(`No identity fields in request — skipping 365-day validation`);
          return {
            body: {
              queryKey: JSON.stringify(parsedQueryKey),
              queryData: body?.queryData
            }
          };
        }

        // Identity fields present — enforce 365-day cooldown
        if (!nomineeData) {
          return buildErrorResponse("No data for this nominee exists", 9812);
        }

        const modifiedDate = new Date(nomineeData.modifiedDate);
        if (isNaN(modifiedDate)) {
          return buildErrorResponse("Invalid modified date format", 400);
        }

        // Calculate 365 days ago (UTC, ignoring time)
        const date365DaysAgo = new Date();
        date365DaysAgo.setUTCDate(date365DaysAgo.getUTCDate() - 365);
        date365DaysAgo.setUTCHours(0, 0, 0, 0);

        // Compare using date only
        const modifiedUTC = new Date(modifiedDate);
        modifiedUTC.setUTCHours(0, 0, 0, 0);

        if ((modifiedUTC).getTime() > date365DaysAgo.getTime()) {
          logger.info(`Update nominee failed [9813] | modifiedDate: ${modifiedDate.toISOString()} within 365 days`);
          return buildErrorResponse(
            "Nominee identity fields can only be edited if last modification was more than 365 days ago",
            9813
          );
        }

        return {
          body: {
            queryKey: JSON.stringify({
              ...parsedQueryKey,
              modifiedDate: { $lt: date365DaysAgo },
              isActive: true
            }),
            queryData: body?.queryData
          }
        };
      }
    };
  }

  @Script({ pos: { x: 484.92161494954416, y: -38.29691215006511 } })
  @Relation(r => dao.isSuccess(), 'validateRequestSchema')
  async StaticConfiguration() {
    const script = {

        execute: async () => {
            let nomineeXApiKeyHeader = await getValueByKey("NOMINEE_X_API_KEY")
            const literals = {
                nomineeXApiKey: nomineeXApiKeyHeader,
            }

            return {
                body: literals
            };

        }
    }
  }
}

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getEffectiveHeaders, getError, getOut, getQueryParams } = dao;

@Dag({ method: "GET", url: "nominee-get" })
class GetNomineeApi {
  constructor() {
    this.vaildateParameters();
  }

  @Schema({ pos: { x: 232.19107328800305, y: -59.73231445984584 } })
  @Relation(r => (dao.hasError()), 'ValidationFailureBlock')
  @Relation(r => (dao.isSuccess()), 'MongoGetSpec')
  async vaildateParameters() {
    return {
      definitions: [],
      spec: {
        type: 'object',
        properties: {
          queryParams: {
            type: 'object',
            // minProperties: 1,
            // maxProperties: 1,
            properties: {
              externalId: {
                type: 'string',
                minLength: 1,
                transform: ['trim'],
                errorMessage: {
                  minLength: "externalId cannot be empty"
                }
              },
            },
            // additionalProperties: false,
            required: ['externalId'],
          }
        },
        required: ['queryParams'],
        errorMessage: {
          required: {
            queryParams: 'queryParams are missing',
          },
        },
      },
    }
  }

  @Script({ pos: { x: 554.1129745878252, y: 93 } })
  @Relation(r => dao.isSuccess(), 'MongoGetNomineeBlock')
  async MongoGetSpec() {
    const script = {
      execute: () => {
        const [identifierType, identifierValue] = Object.entries(getQueryParams())?.[0];
        let mongoQuery = { identifierType, identifierValue, isActive: true };
        return {
          headers: getEffectiveHeaders(),
          body: {
            query: JSON.stringify(mongoQuery)
          },
        };
      },
    };
  }

  @GetMongo({ pos: { x: 861.3389237634756, y: 85.22594917565044 } })
  @Relation(r => dao.isSuccess(), 'DBResponseHandling')
  async MongoGetNomineeBlock() {
  return {
        collectionName: `NomineeDetails`,
        query: r => getBody("MongoGetSpec")?.body?.query,
        sort: ``,
      };
  }

  @Script({ pos: { x: 552.1901777631355, y: -59.73231445984584 } })
  async ValidationFailureBlock() {
    const script = {
      execute: () => {
        const errorArray = [];
        const validationErrors = getError("vaildateParameters")?.err;
        validationErrors?.forEach((validationError) => {
          const error = {
            status: false,
            message: `${validationError.message} at ${validationError.instancePath}`,
            code: 1006,
          };
          errorArray.push(error);
        });

        return {
          http: {
            res: {
              json: {
                errors: errorArray,
              },
              status: 400,
              headers: getEffectiveHeaders()
            }
          }
        };
      },
    };
  }

  @Script({ pos: { x: 1180.1678591233529, y: 81.88792093704248 } })
  async DBResponseHandling() {
    const script = {
      execute: () => {
        const [identifierType, identifierValue] = Object.entries(getApiRequest().queryParams)?.[0];
        let nomineeList = getOut("MongoGetNomineeBlock")?.map((ele) => {
          let { gender, firstName, lastName, dob, createdDate } = ele;
          return {
            nomineeId: Object.values(ele._id.buffer)
              .map((byte) => byte.toString(16).padStart(2, '0'))
              .join(''),
            gender: gender ? gender : '',
            firstName: firstName ? firstName : '',
            lastName: lastName ? lastName : '',
            dob: dob ? dob : '',
            createdDate: createdDate ? createdDate : '',
          };
        });
        if (nomineeList?.length > 0) {
          return {
            http: {
              res: {
                json: {
                  messageCode: '200',
                  message: 'Successful',
                  identifierType,
                  identifierValue,
                  nominee: nomineeList,
                },
                status: 200
              }
            }
          };
        } else {
          return {
            http: {
              res: {
                json: {
                  errors: [
                    {
                      success: false,
                      code: 1007,
                      message: 'No nominee available',
                    },
                  ]
                },
                status: 200
              }
            }
          };
        }
      },
    };
  }
}

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getError, getOut } = dao;

@Dag({ method: "GET", url: "bookings" })
class GetPNRDetails {
  constructor() {
    this.queryParamsValidationBlock();
  }

  @Script({ pos: { x: 487.1829802669182, y: -239.06634964203266 } })
  async validationFailureBlock() {
    const script = {
      execute: () => {
        let errorArray = [];
        const validationErrors = getError("queryParamsValidationBlock")?.err;
        validationErrors?.forEach((validationError) => {
          let path = validationError?.params?.additionalProperty ? ` '${validationError?.params?.additionalProperty}' ` : ' '
          const error = {
            status: false,
            message: `${validationError.message} ${path}at ${validationError.instancePath}`,
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

  @Script({ pos: { x: 482.4014166036068, y: 34.96513073188876 } })
  @Relation(r => (dao.isSuccess() && dao.getBody("retrieveTransactions")?.body?.invalidValueErrors?.length), 'InvalidValuesErrorBlock')
  @Relation(r => (dao.isSuccess() && !dao.getBody("retrieveTransactions")?.body?.invalidValueErrors?.length), 'MongoGetPNRTransactions')
  async retrieveTransactions() {
    const script = {
      execute: () => {
        let errors = [];
        let utcStartDate = null;
        let utcEndDate = null;
        const options = {};
        const { externalId, pnrnumber, start_date, end_date, limit, offset } = getApiRequest()?.queryParams;

        let query = {
          is_active: true,
          flight_status: { $not: { $regex: '^utilized$', $options: 'i' } },
        };

        if (externalId) {
          query.identifier_value = externalId;
        }

        if (pnrnumber) {
          query.pnr_number = pnrnumber;
        }

        if (start_date || end_date) {
          query.departure_date = {};
          if (start_date) {
            utcStartDate = new Date(Date.parse(start_date + 'Z'));
            query.departure_date.$gte = utcStartDate;
          }
          if (end_date) {
            utcEndDate = new Date(Date.parse(end_date + 'Z'));
            query.departure_date.$lte = utcEndDate;
          }
        }
        if (utcStartDate && utcEndDate) {
          if (utcStartDate?.getTime() > utcEndDate?.getTime()) {
            errors.push({
              status: false,
              message: `start_date can't be greater than end_date`,
              code: 1006,
            });
          } else if (utcStartDate?.getTime() == utcEndDate?.getTime()) {
            errors.push({
              status: false,
              message: `start_date can't be equal to end_date`,
              code: 1006,
            });
          }
        }
        if (limit) {
          if (/^-?\d+$/.test(limit)) {
            options.limit = parseInt(limit, 10);
          } else {
            errors.push({
              status: false,
              message: `Invalid limit value. Limit must be a number`,
              code: 1006,
            });
          }
        }
        if (offset) {
          if (/^-?\d+$/.test(offset)) {
            options.skip = parseInt(offset, 10);
          } else {
            errors.push({
              status: false,
              message: `Invalid offset value. Offset must be a number`,
              code: 1006,
            });
          }
        }

        return {
          body: {
            query: JSON.stringify(query),
            options: JSON.stringify(options),
            sort: JSON.stringify({ departure_date: 1 }),
            invalidValueErrors: errors,
          },
        };
      },
    };
  }

  @Schema({ pos: { x: 196.89536951581692, y: -155.863627634237 } })
  @Relation(r => dao.hasError(), 'validationFailureBlock')
  @Relation(r => dao.isSuccess(), 'retrieveTransactions')
  async queryParamsValidationBlock() {
    return {
      definitions: [],
      spec: {
        type: 'object',
        properties: {
          queryParams: {
            type: 'object',
            // maxProperties: 6,
            // minProperties: 1,
            properties: {
              externalId: {
                type: 'string',
                minLength: 1,
                transform: ['trim'],
                errorMessage: {
                  minLength: 'externalId cannot be empty',
                },
              },
              start_date: {
                type: 'string',
                format: "date",
                minLength: 1,
                transform: ['trim'],
                errorMessage: {
                  minLength: 'start_date cannot be empty',
                  format: 'Invalid date format'
                },
              },
              end_date: {
                type: 'string',
                format: "date",
                minLength: 1,
                transform: ['trim'],
                errorMessage: {
                  minLength: 'end_date cannot be empty',
                  format: 'Invalid date format'
                },
              },
              pnrnumber: {
                type: 'string',
                minLength: 1,
                transform: ['trim'],
                errorMessage: {
                  minLength: 'pnrnumber cannot be empty',
                },
              },
              limit: {
                type: 'string',
                minLength: 1,
                errorMessage: {
                  minLength: 'limit cannot be empty',
                },
              },
              offset: {
                type: 'string',
                minLength: 1,
                errorMessage: {
                  minLength: 'offset cannot be empty',
                },
              },
            },
            additionalProperties: false,
            required: ['externalId'],
          },
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

  @Script({ pos: { x: 747.3118050345113, y: -107.13979052627478 } })
  async InvalidValuesErrorBlock() {
    const script = {
      execute: () => {
        let errorArray = [];
        if (getBody("retrieveTransactions")?.body?.invalidValueErrors?.length) {
          errorArray = [...errorArray, ...getBody("retrieveTransactions")?.body?.invalidValueErrors];
        }

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

  @GetMongo({ pos: { x: 765.7348285579886, y: 137.18713376815737 } })
  @Relation(r => dao.isSuccess(), 'DBResponseHandling')
  async MongoGetPNRTransactions() {
  return {
        collectionName: `PNR_Transactions`,
        query: r => getBody("retrieveTransactions")?.body?.query,
        sort: `{ "departure_date": -1 }`,
        options: r => getBody("retrieveTransactions")?.body?.options,
      };
  }

  @Script({ pos: { x: 1033.9096241393574, y: 134.98019899575146 } })
  async DBResponseHandling() {
    const script = {
      execute: () => {
        const simulationDetails = getOut("MongoGetPNRTransactions")?.map((item) => {
          let responseFromMongo = {...(item)};
          let simulationResponse = responseFromMongo.simulation_response;
          simulationResponse['date_created'] = responseFromMongo.date_created
          simulationResponse['date_updated'] = responseFromMongo.date_updated
          return simulationResponse;
        });

        if (simulationDetails?.length) {
          return {
            http: {
              res: {
                json: simulationDetails,
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
                      status: false,
                      message: `No bookings found`,
                      code: 1007,
                    }
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

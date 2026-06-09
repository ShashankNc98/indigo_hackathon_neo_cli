import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest } = dao;

@Dag({ method: "PUT", url: "cms" })
class UpsertCmsData {
  constructor() {
    this.SchemaValidation();
  }

  @Script({ pos: { x: 320, y: 0 } })
  @Relation(r => dao.isSuccess(), 'InsertCmsSchema')
  async PrepareCmsInsertion() {
    const script = {

        execute: () => {
            let currentDate = new Date().toISOString();
            const requestBody = getApiRequest().body;

            const partner = requestBody.partner;
            const connectPlusDataflowId = requestBody.connectPlusDataflowId; 

            delete requestBody['modifiedDate'];
            delete requestBody['creationDate'];

            const upsertFields = {
                ...(requestBody),
                modifiedDate: currentDate,
            };

            let upsertQuery = JSON.stringify({
                $set: {
                    ...upsertFields
                },
                $setOnInsert: {
                    "creationDate": currentDate
                }
            });

            let upsertQueryKey = {
                partner,
                connectPlusDataflowId
            };

            return {
                body: {
                    "query": upsertQuery,
                    "queryKey": upsertQueryKey
                }
            }

        }
    }
  }

  @Schema({ pos: { x: 78, y: 14 } })
  @Relation(r => dao.isSuccess(), 'PrepareCmsInsertion')
  async SchemaValidation() {
    return {
      definitions: [],
      spec: {
        type: "object",
        properties: {
          body: {
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
              },
              isActive: {
                type: 'boolean',
                transform: ['trim', 'toLowerCase'],
                errorMessage: {
                  type: "isActive must be a boolean"
                }
              }
            },
            required: ['connectPlusDataflowId', 'partner', 'isActive'],
            errorMessage: {
              required: {
                connectPlusDataflowId: "connectPlusDataflowId is missing",
                partner: "partner is missing",
                isActive: "isActive is missing"
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

  @PutMongo({ pos: { x: 640, y: 0 } })
  async InsertCmsSchema() {
  return {
        collectionName: `CMS_Schema`,
        mode: `upsert`,
        query: r => getBody().body.query,
        queryKey: r => getBody().body.queryKey,
      };
  }
}

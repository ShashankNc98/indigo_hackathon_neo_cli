import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest } = dao;

@Dag({ method: "POST", url: "hdfcTransformation" })
class HdfcTransformation {
  constructor() {
    this.HdfcScript();
  }

  @Script({ pos: { x: 320, y: 1 } })
  async HdfcScript() {
    const script = {

        execute: () => {
            let input = getApiRequest().body?.[0];
            let milestonePayload = {
                "ffn" : input['FFN'],
                "productName": input['Product Name'],
                "typeOfBenefit": input['Type Of Benefit']
            }
            //Write your code here.
            return {
                http: {
                    res: {
                        json: milestonePayload
                    }
                }
            };

        }
    }
  }
}

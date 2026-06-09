import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getEffectiveHeaders } = dao;

@Dag({ method: "POST", url: "dbconfig" })
class DbConfigManagement {
  constructor() {
    this.queryParams();
  }

  @Script({ pos: { x: 83, y: -107 } })
  @Relation(r => dao.isSuccess(), 'emailDomainBlacklistUpdate')
  async queryParams() {
    const script = {

        execute: () => {
            const requestQueryParams = Object.keys(getApiRequest()?.queryParams)
            const data = getApiRequest()?.body;
            // testing 

                var flag = true;
                if(flag){
                    return {
                    body: {
                        updateQuery: JSON.stringify(data),
                        collectionName: requestQueryParams[0]
                    }
                }
                }

            // end testing

            const updateQuery = JSON.stringify({
                $addToSet: {...data}
            });
           const queryKey = JSON.stringify({});

            //Write your code here.
            return {
                //headers: getEffectiveHeaders(),
                body: {
                    // requestQueryParams,
                    // query: data,
                    collectionName: "email",
                    updateQuery,
                    queryKey,
                    options : { upsert: true , new : true}
                }
            };

        }
    }
  }

  @PutMongo({ pos: { x: 548, y: -127 } })
  async emailDomainBlacklistUpdate() {
  return {
        collectionName: `emailDomainBlacklist`,
        mode: `upsert`,
        query: r => getBody().body.updateQuery,
        queryKey: `{}`,
        options: ``,
      };
  }
}

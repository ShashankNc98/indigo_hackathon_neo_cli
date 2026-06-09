import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest } = dao;

@Dag({ method: "POST", url: "tierForceUpgrade" })
class TierForceUpgrade {
  constructor() {
    this.TransformPayload();
  }

  @Script({ pos: { x: 303, y: -2 } })
  @Relation(r => dao.isSuccess(), 'PlatformApi')
  async TransformPayload() {
    const script = {

        execute: () => {
            const requestBody=getApiRequest().body;
            const requestHeaders=getApiRequest().headers;
            const loyaltyType="loyalty";
            const type="externalId";
            const source="INSTORE";

            delete requestHeaders["x-cap-neo-test-variant-id"]

            const headers = {
                "X-CAP-API-OAUTH-TOKEN": requestHeaders["x-cap-api-oauth-token"] || requestHeaders["X-CAP-API-OAUTH-TOKEN"],
                "X-CAP-API-ATTRIBUTION-entity-TYPE": requestHeaders["X-CAP-API-ATTRIBUTION-entity-TYPE"] || requestHeaders["x-cap-api-attribution-entity-type"],
                "X-CAP-API-ATTRIBUTION-entity-CODE": requestHeaders["X-CAP-API-ATTRIBUTION-entity-CODE"] || requestHeaders["x-cap-api-attribution-entity-code"],
                "Content-Type": requestHeaders["Content-Type"] || requestHeaders["content-type"]

            };

            const body = {
                loyaltyInfo: {
                    loyaltyType: loyaltyType 
                },
                profiles: [
                    {
                        identifiers: [
                            {
                                type: type,
                                value: requestBody["externalId"] || requestBody[0]["externalId"]
                            }
                        ],
                        source: source,           
                        fields: {
                            forceupgrade: requestBody["fields_forceupgrade"] || requestBody[0]["fields_forceupgrade"]
                     }
                    }
                ]
            };

            const queryParameters={
                identifierName:type,
                identifierValue:requestBody["externalId"] || requestBody[0]["externalId"],
                source:source
            }

            //Write your code here.
            return {
                headers,
                queryParams:queryParameters,
                body:JSON.stringify(body)
            };

        }
    }
  }

  @ApiRequest({ pos: { x: 623, y: -2 } })
  async PlatformApi() {
  return {
        url: `https://apac.api.capillarytech.com/v2/customers/lookup`,
        method: `PUT`,
      };
  }
}

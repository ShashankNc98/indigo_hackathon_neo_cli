import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

@Dag({ method: "GET", url: "someUrl" })
class PartnerNameValidations {
  constructor() {
    this.defaultSchema();
  }

  @Schema({ pos: { x: 0, y: 0 } })
  async defaultSchema() {
    return {}
  }
}

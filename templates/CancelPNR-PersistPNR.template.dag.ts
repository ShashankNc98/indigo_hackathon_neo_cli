// PLACEHOLDERS (4) — replace with values from your <airline>.config.json
// {{DAG_URL}}                            — e.g. "cancelPNRPersist" (internal)
// {{MONGO_PNR_TRANSACTIONS_COLLECTION}}  — e.g. "PNR_Transactions"
// {{MONGO_UTILISED_PNR_COLLECTION}}      — e.g. "UtilisedPNR"
// {{APP_VERSION}}                        — e.g. "1.0.0"

import dao from 'neo/dao';
import logger from 'neo/logger';
import { Dag, Script, ApiRequest, Schema, GetMongo, PutMongo,
  BulkMongo, Encrypt, Decrypt, Hmac, Kafka, RedisEvict, ValidateSignature,
  CacheManager, Cachable, ExecutionStrategy, Relation } from 'neo/decorators';

const { getApiRequest, getBody, getEffectiveHeaders, getIn, getOut } = dao;

@Dag({ method: "POST", url: "{{DAG_URL}}" })
class CancelPNRPersistPNR {
  constructor() {
    this.AppConfigurations();
  }

  @Script({ pos: { x: -400, y: 0 } })
  @Relation(r => dao.isSuccess(), 'BuildCancellationMongoQueries')
  async AppConfigurations() {
    const script = {
      execute: () => {
        const appVersion = "{{APP_VERSION}}";
        logger.info(`APP_VERSION:${JSON.stringify(appVersion)}`);
        return { body: { APP_VERSION: appVersion } };
      }
    }
  }

  @Script({ pos: { x: 0, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpdatePNRTransactionsCollection')
  async BuildCancellationMongoQueries() {
    const script = {
      execute: () => {
        const payload = getApiRequest().body;
        const billNumbers = payload.billNumbers || [];
        const cancellationDate = new Date();
        const queries = billNumbers.map(bn => ({
          body: {
            query: JSON.stringify({ $set: { flight_status: "CANCELLED", flight_status_updation_date: cancellationDate, is_active: false } }),
            queryKey: JSON.stringify({ bill_number: bn })
          }
        }));
        logger.info(`Building cancellation queries for ${billNumbers.length} bill numbers`);
        return { status: 200, body: { queries, billNumbers, cancellationDate } };
      }
    }
  }

  @PutMongo({ pos: { x: 400, y: 0 } })
  @Relation(r => dao.isSuccess(), 'UpdateUtilisedPNRCollection')
  async UpdatePNRTransactionsCollection() {
    return {
      collectionName: `{{MONGO_PNR_TRANSACTIONS_COLLECTION}}`,
      mode: `update`,
      query: r => getBody().body.query,
      queryKey: r => getBody().body.queryKey,
    };
  }

  @Script({ pos: { x: 700, y: 0 } })
  @Relation(r => dao.isSuccess(), 'MarkUtilisedPNRInactive')
  async UpdateUtilisedPNRCollection() {
    const script = {
      execute: () => {
        const billNumbers = getBody("BuildCancellationMongoQueries").body.billNumbers;
        const cancellationDate = getBody("BuildCancellationMongoQueries").body.cancellationDate;
        return billNumbers.map(bn => ({
          body: {
            query: JSON.stringify({ $set: { isActive: false, modifiedDate: cancellationDate, flight_status: "CANCELLED" } }),
            queryKey: JSON.stringify({ billNumber: bn })
          }
        }));
      }
    }
  }

  @PutMongo({ pos: { x: 1000, y: 0 } })
  @Relation(r => dao.isSuccess(), 'CancellationPersistenceResponse')
  async MarkUtilisedPNRInactive() {
    return {
      collectionName: `{{MONGO_UTILISED_PNR_COLLECTION}}`,
      mode: `update`,
      query: r => getBody().body.query,
      queryKey: r => getBody().body.queryKey,
    };
  }

  @Script({ pos: { x: 1300, y: 0 } })
  async CancellationPersistenceResponse() {
    const script = {
      execute: () => {
        const billNumbers = getBody("BuildCancellationMongoQueries").body.billNumbers;
        logger.info(`Cancellation persisted for ${billNumbers.length} record(s)`);
        return { http: { res: { status: 200, json: { status: true, message: "Cancellation persisted successfully", updatedCount: billNumbers.length },
          headers: { "App-Version": getBody("AppConfigurations")?.body.APP_VERSION } } } };
      }
    }
  }
}

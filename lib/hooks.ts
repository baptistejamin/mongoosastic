import { MongoosasticDocument } from './types'
import { flatten, unflatten } from 'flat'
import { bulkUpdate } from './bulking'
import { mongoSetToScript, mongoConditionToQuery } from './utils'

export async function postSave(doc: MongoosasticDocument): Promise<void> {
  if (!doc) {
    return
  }

  const options = doc.esOptions()

  const filter = options && options.filter

  function onIndex(err: unknown, res: unknown) {
    if (!filter || !filter(doc)) {
      doc.emit('es-indexed', err, res)
    } else {
      doc.emit('es-filtered', err, res)
    }
  }

  const populate = options && options.populate
  if (doc) {
    if (populate && populate.length) {
      const popDoc = await doc.populate(populate)
      popDoc
        .index()
        .then((res) => onIndex(null, res))
        .catch((err) => onIndex(err, null))
    } else {
      doc
        .index()
        .then((res) => onIndex(null, res))
        .catch((err) => onIndex(err, null))
    }
  }
}

export function postRemove(doc: MongoosasticDocument): void {
  if (!doc) {
    return
  }

  doc.unIndex()
}

export function postUpdate(query: any, doc: MongoosasticDocument, schema: any ): void {
  const conditions = query._conditions
  const update = query._update
  const client = schema.statics.esClient()
  const options = schema.statics.esOptions() || {}
  const indexName = options.index || query._collection.collection.collectionName

  const $query = flatten(conditions || {})
  const $set = unflatten(update.$set)

  const script = mongoSetToScript($set as Record<string, unknown>)
  const esQuery = mongoConditionToQuery($query as Record<string, unknown>)

  if (options.bulk && options.idMapper) {
    const opt = {
      index: indexName,
      id: options.idMapper(unflatten(conditions)),
      body: {
        script: script
      },
      bulk: options.bulk
    }

    bulkUpdate({ model: query.model, ...opt })
  } else {
    client.updateByQuery({
      index: indexName,
      body: {
        query: esQuery,
        script: script,
        conflicts: 'proceed'
      }
    })
  }
}

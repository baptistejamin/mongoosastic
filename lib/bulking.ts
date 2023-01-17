import {
  BulkIndexOptions,
  BulkInstruction,
  BulkOptions,
  BulkUnIndexOptions,
  MongoosasticDocument,
  MongoosasticModel,
  SchemaWithInternals
} from './types'

declare type BulkStoreValue = {
  buffer:  BulkInstruction[],
  timeout: NodeJS.Timeout | undefined
};

const bulkStore: Map<string, BulkStoreValue> = new Map()

function clearBulkTimeout(model: MongoosasticModel<MongoosasticDocument>) {
  const store = getStore(model)

  clearTimeout(store.timeout as NodeJS.Timeout)
  store.timeout = undefined
}

function getStore(model: MongoosasticModel<MongoosasticDocument>) : BulkStoreValue {
  const $id = (model.schema as SchemaWithInternals).$id

  if (!bulkStore.has($id)) {
    bulkStore.set($id, {
      buffer: [],
      timeout: undefined
    } as BulkStoreValue)
  }

  // eslint-disable-next-line @typescript-eslint/no-extra-non-null-assertion, @typescript-eslint/no-non-null-assertion
  return bulkStore.get($id)!!
}

let PARRALEL_REQUESTS = 0

export async function bulkAdd(opts: BulkIndexOptions): Promise<void> {
  const instruction = [
    {
      index: {
        _index: opts.index,
        _id: opts.id,
      },
    },
    opts.body,
  ]

  await bulkIndex(opts.model, instruction, opts.bulk as BulkOptions)
}

export async function bulkUpdate(opts: BulkIndexOptions): Promise<void> {
  const instruction = [
    {
      update: {
        _index: opts.index,
        _id: opts.id,
      },
    },
    opts.body,
  ]

  await bulkIndex(opts.model, instruction, opts.bulk as BulkOptions)
}

export async function bulkDelete(opts: BulkUnIndexOptions): Promise<void> {
  const instruction = [
    {
      delete: {
        _index: opts.index,
        _id: opts.id,
      },
    },
  ]

  await bulkIndex(opts.model, instruction, opts.bulk as BulkOptions)
}

export async function bulkIndex(
  model: MongoosasticModel<MongoosasticDocument>,
  instruction: BulkInstruction[],
  bulk: BulkOptions
): Promise<void> {
  const store = getStore(model)

  store.buffer = store?.buffer.concat(instruction)

  if (store.buffer.length >= bulk.size) {
    await model.flush()
    clearBulkTimeout(model)
  } else if (store.timeout === undefined) {
    store.timeout = setTimeout(async () => {
      await model.flush()
      clearBulkTimeout(model)
    }, bulk.delay)
  }
}

export async function flush(this: MongoosasticModel<MongoosasticDocument>): Promise<void> {
  const start = Date.now()
  const store = getStore(this)

  //console.log(store.buffer)

  PARRALEL_REQUESTS++

  console.log('currenting having ' + PARRALEL_REQUESTS + ' requests')

  this.esClient().child()
    .bulk({
      body: store.buffer,
      filter_path: 'took,items.**.error'
    })
    .then((res) => {
      console.log('took '  + res.body.took + ' but real is ' + (Date.now() - start).toString())
      PARRALEL_REQUESTS--
      if (res.body.items && res.body.items.length) {
        for (let i = 0; i < res.body.items.length; i++) {
          const info = res.body.items[i]

          if (info && info.update && info.update.error) {
            console.log(info.update.error)
            this.bulkError().emit('error', null, info.update)
          }

          if (info && info.index && info.index.error) {
            this.bulkError().emit('error', null, info.index)
          }

          if (info && info.delete && info.delete.error) {
            this.bulkError().emit('error', null, info.index)
          }
        }
      }
    })
    .catch((error) => {
      PARRALEL_REQUESTS--
      console.log('got an error', error)
      this.bulkError().emit('error', error, null)
    })

  store.buffer = []
}

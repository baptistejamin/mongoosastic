import { flatten, unflatten } from 'flat'
import { isArray, merge } from 'lodash'
import { v4 as uuidv4 } from 'uuid'

type PainlessScript = {
  lang: 'painless';
  source: string;
  params?: Record<string, unknown>;
}

type AddSetField = {
  stack: unknown[];
  id: string
  deduplicate: boolean,
  path: string
}

const BRACKET_NOTATION_REGEX = /\.\[/g
const INLINE_SCRIPT_REGEX = /\n\s{1,}/g
const BRACKETS_SPLIT_REGEX = /\['[^[\]]*'\]/gm

export default class ConversionGenerator {
  assertFields: string[] = []
  parameters: Record<string, unknown> = {}
  sources: string[] = []

  $set(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(fieldsMap, {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    // Store parameters
    merge(this.parameters, unflatten(fieldsMap))

    // Use bracket notation a.b.c => a['b]['c]
    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    this.assertFields =  this.assertFields.concat(brackets)

    const source = brackets.map(bracket => `ctx._source${bracket} = params${bracket};`).join(' ')

    // Store sources
    this.sources.push(source)
  }

  $unset(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(unflatten(fieldsMap), {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    let source = ''

    // Foreach key in the map
    for (const bracket of brackets) {
      // Split brackets. ["['a']", "['b']", "['c']"]
      const match = bracket.match(BRACKETS_SPLIT_REGEX)

      let assertKey = ''

      if (!match) {
        continue
      }

      let condition = ''

      // a.b.c.d -> key to delete will be d on path a.b.c
      const keyToDelete = match[match.length - 1].replace('[\'', '').replace('\']', '')

      // a && a.b && a.b.c
      for (let i = 0; i < match.length - 1; i++) {
        const currentMatch = match[i]

        if (condition != '') {
          condition += ' && '
        }

        assertKey += currentMatch
        condition += ` ctx._source${assertKey} != null `
      }

      if (keyToDelete) {
        // No condition if we are applying remove on the root level
        if (condition) {
          source += `if (${condition}) {`
        }

        // ctx._source['a']['b']['c'].remove('d')
        source += `ctx._source${assertKey}.remove('${keyToDelete}');`

        if (condition) {
          source += '} '
        }
      }
    }

    this.sources.push(source)
  }

  $addToSet(fieldsMap: Record<string, unknown> = {}) {
    const _fields : Record<string, unknown> = flatten(unflatten(fieldsMap), {
      safe: true,
      transformKey: key => `['${key}']`,
    })

    const addSetMap : Map<string, AddSetField> = new Map()

    const brackets = Object.keys(_fields).map(this.convertToBracketNotation)

    let source = ''

    // Foreach key in the map
    for (const bracket of brackets) {
      // Split brackets. ["['a']", "['b']", "['c']"]
      const match = bracket.match(BRACKETS_SPLIT_REGEX)

      let assertKey = ''

      if (!match) {
        continue
      }

      let condition = ''
      let currentKey = ''
      let keyToAdd = ''
      const assertFields = []

      for (let i = 0; i < match.length; i++) {
        const currentMatch = match[i]

        assertFields.push(match[i])

        // a, then a.b, then a.b.c
        if (currentKey) {
          currentKey += '.'
        }

        currentKey += currentMatch.replace('[\'', '').replace('\']', '')

        // break the pipline as soon as a current key (a.b.c) is found in the fieldsMap
        if (currentKey && fieldsMap[currentKey]) {
          keyToAdd = currentKey

          break
        }


        if (condition != '') {
          condition += ' && '
        }

        assertKey += currentMatch
        condition += ` ctx._source${assertKey} != null `
      }

      const newSet : AddSetField = {
        id: uuidv4(),
        stack: [],
        path: assertKey,
        deduplicate: false
      }
      
      if ((fieldsMap[keyToAdd] as  Record<string, unknown>).$each) {
        if (isArray((fieldsMap[keyToAdd] as  Record<string, unknown>).$each)) {
          newSet.stack = (fieldsMap[keyToAdd] as  Record<string, unknown>).$each as unknown[]
          newSet.deduplicate = true
        }
      } else {
        newSet.stack = [(fieldsMap[keyToAdd] as  Record<string, unknown>)]
      }

      addSetMap.set(keyToAdd, newSet)
  
      this.parameters[newSet.id] = newSet.stack
    }

    addSetMap.forEach(addSetField => {
      if (addSetField.deduplicate) {
        source += `
          for (value in params['${addSetField.id}']) {
            if (!ctx._source${addSetField.path}.containsKey(value.toString())) {
              ctx._source${addSetField.path}.add(value)
            }
          }
        `
      } else {
        source += `
          ctx._source${addSetField.path}.addAll(params['${addSetField.id}'])
        `
      }
    })

    this.assertFields =  this.assertFields.concat(brackets)

    this.sources.push(source)
  }

  build() : PainlessScript {
    return {
      lang: 'painless',
      source: this.assertNullKeys(this.assertFields) + this.sources.join(' '),
      params: this.parameters,
    }
  }

  private convertToBracketNotation(key: string): string {
    return key.replace(BRACKET_NOTATION_REGEX, '[')
  }

  private convertMultilineScriptToInline(script: string): string {
    return script.replace(INLINE_SCRIPT_REGEX, ' ').trim()
  }

  private assertNullKeys(brackets: string[]): string {
    let result = ''

    for (const bracket of brackets) {
      const match = bracket.match(BRACKETS_SPLIT_REGEX)

      let assertKey = ''

      if (!match) {
        continue
      }

      for (let i = 0; i < match.length - 1; i++) {
        const currentMatch = match[i]

        assertKey += currentMatch

        result += `if (ctx._source${assertKey} == null) {
          ctx._source${assertKey} = [:]
        }
        `
      }
    }

    return this.convertMultilineScriptToInline(result)
  }
}
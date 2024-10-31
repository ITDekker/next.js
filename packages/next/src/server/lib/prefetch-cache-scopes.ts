import type { ResumeDataCache } from '../use-cache/resume-data-cache'

export class PrefetchCacheScopes {
  private cacheScopes = new Map<
    string,
    {
      cache: ResumeDataCache
      // we track timestamp as we evict after 30s
      // if a prefetch cache scope isn't used by then
      timestamp: number
    }
  >()

  private evict() {
    for (const [key, value] of this.cacheScopes) {
      if (value.timestamp < Date.now() - 5_000) {
        this.cacheScopes.delete(key)
      }
    }
  }

  // TODO: should this key include query params if so we need to
  // filter _rsc query
  get(url: string) {
    setImmediate(() => this.evict())
    const currentScope = this.cacheScopes.get(url)
    if (currentScope) {
      if (currentScope.timestamp < Date.now() - 5_000) {
        return undefined
      }
      return currentScope.cache
    }
    return undefined
  }

  set(url: string, cache: ResumeDataCache) {
    setImmediate(() => this.evict())
    return this.cacheScopes.set(url, {
      cache,
      timestamp: Date.now(),
    })
  }

  del(url: string) {
    this.cacheScopes.delete(url)
  }
}

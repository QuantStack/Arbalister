export class PairMap<K1, K2, V> {
  private map = new Map<string, V>();

  set(key: [K1, K2], value: V): this {
    this.map.set(JSON.stringify(key), value);
    return this;
  }

  get(key: [K1, K2]): V | undefined {
    return this.map.get(JSON.stringify(key));
  }

  clear(): void {
    this.map.clear();
  }

  delete(key: [K1, K2]): boolean {
    return this.map.delete(JSON.stringify(key));
  }

  has(key: [K1, K2]): boolean {
    return this.map.has(JSON.stringify(key));
  }

  get size(): number {
    return this.map.size;
  }

  forEach(
    callbackfn: (value: V, key: [K1, K2], map: PairMap<K1, K2, V>) => void,
    // biome-ignore lint/suspicious/noExplicitAny: This is in the Map signature
    thisArg?: any,
  ): void {
    this.map.forEach((value, key) => {
      callbackfn.call(thisArg, value, JSON.parse(key), this);
    });
  }
}

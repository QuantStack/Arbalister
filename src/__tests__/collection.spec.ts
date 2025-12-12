import { PairMap } from "../collection";

describe("PairMap", () => {
  it("sets and gets values with primitive keys", () => {
    const map = new PairMap<number, string, string>();
    map.set([1, "a"], "value1");
    expect(map.get([1, "a"])).toBe("value1");
    expect(map.get([2, "a"])).toBeUndefined();
  });

  it("checks has, delete, and clear", () => {
    const map = new PairMap<number, number, string>();
    map.set([1, 2], "foo");
    expect(map.has([1, 2])).toBe(true);
    expect(map.delete([1, 2])).toBe(true);
    expect(map.has([1, 2])).toBe(false);
    map.set([3, 4], "bar");
    map.clear();
    expect(map.size).toBe(0);
  });

  it("returns correct size", () => {
    const map = new PairMap<string, string, number>();
    map.set(["a", "b"], 1);
    map.set(["c", "d"], 2);
    expect(map.size).toBe(2);
    map.delete(["a", "b"]);
    expect(map.size).toBe(1);
  });

  it("overwrites values for same key", () => {
    const map = new PairMap<number, number, string>();
    map.set([1, 2], "first");
    map.set([1, 2], "second");
    expect(map.get([1, 2])).toBe("second");
    expect(map.size).toBe(1);
  });

  it("iterates with forEach", () => {
    const map = new PairMap<number, number, string>();
    map.set([1, 2], "a");
    map.set([3, 4], "b");
    const entries: Array<[[number, number], string]> = [];
    map.forEach((value, key) => {
      entries.push([key, value]);
    });
    expect(entries).toContainEqual([[1, 2], "a"]);
    expect(entries).toContainEqual([[3, 4], "b"]);
    expect(entries.length).toBe(2);
  });
});

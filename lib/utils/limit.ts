export type Limit = <T>(fn: () => Promise<T>) => Promise<T>;

export function createLimit(concurrency: number): Limit {
  let activeCount = 0;
  const queue: Array<() => void> = [];

  const next = () => {
    activeCount--;
    const run = queue.shift();
    if (run) run();
  };

  const runFn = async <T>(
    fn: () => Promise<T>,
    resolve: (p: Promise<T>) => void
  ) => {
    activeCount++;
    const p = (async () => fn())();
    resolve(p);
    try {
      await p;
    } finally {
      next();
    }
  };

  return <T>(fn: () => Promise<T>) =>
    new Promise<Promise<T>>((resolve) => {
      const task = () => void runFn(fn, resolve);
      if (activeCount < concurrency) task();
      else queue.push(task);
    }).then((p) => p);
}

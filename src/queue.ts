export class AsyncQueue<T> implements AsyncIterable<T>, AsyncIterator<T> {
  private readonly values: T[] = [];
  private readonly resolvers: Array<(result: IteratorResult<T>) => void> = [];
  private closed = false;

  public push(value: T): void {
    if (this.closed) {
      throw new Error("Queue is closed");
    }

    const resolver = this.resolvers.shift();
    if (resolver) {
      resolver({ value, done: false });
      return;
    }

    this.values.push(value);
  }

  public close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;

    for (const resolver of this.resolvers.splice(0)) {
      resolver({ value: undefined as T, done: true });
    }
  }

  public next(): Promise<IteratorResult<T>> {
    const value = this.values.shift();
    if (value !== undefined) {
      return Promise.resolve({ value, done: false });
    }

    if (this.closed) {
      return Promise.resolve({ value: undefined as T, done: true });
    }

    return new Promise((resolve) => {
      this.resolvers.push(resolve);
    });
  }

  public [Symbol.asyncIterator](): AsyncIterator<T> {
    return this;
  }
}

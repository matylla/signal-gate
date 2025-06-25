export default class CircularBuffer {
  /**
   * @param {number} capacity  Max number of elements to hold
   */
  constructor(capacity) {
    this.capacity = capacity;
    this.buffer = [];
  }

  /**
   * Add an item; if over capacity, drop the oldest.
   * @param {*} item
   */
  add(item) {
    if (this.buffer.length === this.capacity) {
      this.buffer.shift();
    }
    this.buffer.push(item);
  }

  /**
   * Get item at index i (0 = oldest, length‐1 = newest).
   * @param {number} i
   */
  get(i) {
    return this.buffer[i];
  }

  /**
   * Get the newest (most recent) item.
   */
  getNewest() {
    return this.buffer[this.buffer.length - 1];
  }

  /**
   * Return an array copy of the buffer.
   */
  toArray() {
    return this.buffer.slice();
  }

  /**
   * Number of items currently in buffer.
   */
  get size() {
    return this.buffer.length;
  }
}
import { Readable } from 'node:stream';
import { streamToBuffer, validateStream } from '../../src/utils/stream-utils';

describe('StreamUtils', () => {
  describe('streamToBuffer', () => {
    it('should convert a readable stream to buffer', async () => {
      const testData = 'Hello, World!';
      const stream = Readable.from([testData]);

      const buffer = await streamToBuffer(stream);

      expect(buffer).toBeInstanceOf(Buffer);
      expect(buffer.toString()).toBe(testData);
    });

    it('should handle multiple chunks', async () => {
      const chunks = ['Hello', ', ', 'World', '!'];
      const stream = Readable.from(chunks);

      const buffer = await streamToBuffer(stream);

      expect(buffer.toString()).toBe('Hello, World!');
    });

    it('should handle empty stream', async () => {
      const stream = Readable.from([]);

      const buffer = await streamToBuffer(stream);

      expect(buffer).toBeInstanceOf(Buffer);
      expect(buffer.length).toBe(0);
    });
  });

  describe('validateStream', () => {
    it('should return stream if valid', () => {
      const stream = Readable.from(['test']);

      const result = validateStream(stream, 'Test error');

      expect(result).toBe(stream);
    });

    it('should throw error if stream is null', () => {
      expect(() => {
        validateStream(null, 'Stream is null');
      }).toThrow('Stream is null');
    });

    it('should throw error if stream is undefined', () => {
      expect(() => {
        validateStream(undefined, 'Stream is undefined');
      }).toThrow('Stream is undefined');
    });

    it('should use custom error message', () => {
      const customMessage = 'Custom error message';

      expect(() => {
        validateStream(null, customMessage);
      }).toThrow(customMessage);
    });
  });
});

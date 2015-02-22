# Kinesis Writable

```
npm install kinesis-writable
```

Stream data to an AWS Kinesis stream as fast as possible — but no faster.

Newline are treated as delimiters.

## Example

```js
// stdin.js
var kpw = require('kinesis-writable')
var stream = kpw.create('your-stream-name', {region: 'us-west-2'}, function (data) {
  return doSomethingWithDataToDecidePartitionKey(data)
})
process.stdin.pipe(stream)
```

```
echo -n "hello\nworld" | node stdin.js

# Writes "hello" and "world" as individual records to Kinesis
```

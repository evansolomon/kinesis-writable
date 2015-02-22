async = require 'async'
AWS = require 'aws-sdk'
sculpt = require 'sculpt'
simul = require 'simul'


exports.create = (streamName, awsConfig = {}, createPartitionKey = null) ->
  writable = new KinesisWritable streamName, awsConfig, createPartitionKey

  splitter = sculpt.split '\n'
  writable.on 'error', splitter.emit.bind splitter, 'error'
  splitter.pipe writable
  splitter


exports.KinesisWritable = class KinesisWritable extends simul.Writable
  @createRandomKey: -> Math.random().toString()
  @MAX_RECORD_SIZE: 50 * 1024 # 50 KB
  @MAX_REQUEST_SIZE: 4.5 * 1024 * 1024 # 4.5 MB
  @MAX_RECORDS_PER_REQUEST: 500

  constructor: (@streamName, awsConfig, @createPartitionKey = KinesisWritable.createRandomKey) ->
    super KinesisWritable.MAX_RECORDS_PER_REQUEST

    @kinesis = new AWS.Kinesis awsConfig
    @cargo = async.cargo (records, done) =>
      requestBatches = records.slice(1).reduce (memo, record)  ->
        recordSize = record.Data.length
        lastBatch = memo[memo.length - 1]
        if (lastBatch.size + recordSize) < KinesisWritable.MAX_REQUEST_SIZE
          lastBatch.push record
        else
          memo.push new RequestBatch record

        memo
      , [new RequestBatch records[0]]

      async.each requestBatches, (batch, cb) =>
        params = {StreamName: @streamName, Records: batch.records}
        @kinesis.putRecords params, cb
      , done
    , KinesisWritable.MAX_RECORDS_PER_REQUEST

  _parallelWrite: (data, enc, callback) ->
    if data.length > KinesisWritable.MAX_RECORD_SIZE
      return callback()

    @cargo.push
      Data: data,
      PartitionKey: @createPartitionKey data
    , callback


exports.RequestBatch = class RequestBatch
  constructor: (firstRecord) ->
    @records = []
    @size = 0
    @push firstRecord

  push: (record) ->
    @records.push record
    @size += record.Data.length

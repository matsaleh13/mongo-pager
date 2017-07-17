'use strict';

const chai = require('chai');
const expect = chai.expect;
chai.config.includeStack = true;
const mongodb = require('mongo-mock');
const mongoose = require('mongoose');
const async = require('async');
const util = require('util');

const pager = require('../dist/mongo-pager');

function randomId() {
  return (Date.now().toString(36) + Math.random().toString(36).substr(2, 5)).toUpperCase();    // https://gist.github.com/gordonbrander/2230317
}

const MongoClient = mongodb.MongoClient;
MongoClient.persist = 'mongo.js';  // persist data to disk.

// Create the test schema and model.
const docSchema = new mongoose.Schema({
  valueA: { type: String, required: true },
  valueB: { type: Number, required: true, default: 0 },
  timestamp: { type: Date, required: true, default: () => new Date() }
}, { autoIndex: false });
docSchema.index({ valueA: 1, _id: 1 });
docSchema.index({ valueB: -1, _id: 1 });
docSchema.index({ valueA: 1, valueB: 1, timestamp: 1 }, { unique: true });
docSchema.statics.findWithPaging = function (query, projection, sortKey, pgLimit, pgQuery, done) {
  return pager.findWithPaging(this.collection, query, projection, sortKey, pgLimit, pgQuery, done);
};
// NOTE: don't keep connection in global scope; tests fail (see comment in setup below).
// const conn = db.connection(config.testDb.conn);
// const PagingDocModel = conn.model('PagingDocModel', docSchema);
let PagingDocModel;

const url = 'mongodb://localhost:27017/myproject';

describe('mongo-pager', () => {

  // Sets up new DB connection and updates indexes.
  function setup(done) {
    MongoClient.connect(url, {}, (err, db) => {
      mongoose.connection = db;

      PagingDocModel = mongoose.model('PagingDocModel', docSchema);

      return done(null);
    });
  }

  function teardown(done) {
    mongoose.connection.close(done);
  }

  // Called by Mocha before each test method runs.
  beforeEach(function (done) {
    setup(done);
  });

  // Called by Mocha after each test method runs.
  afterEach(function (done) {
    teardown(done);
  });

  describe('.findWithPaging', function () {

    /**
     * Creates a number of documents for use in testing.
     *
     * @param {Number} [numDocs] Number of documents to create.
     * @param {Function} [fnCreate] Creation function for each document.
     * @param {Function} [done] Function to call when operation completes. Param: (err, documents).
     */
    function suiteSetup(numDocs, fnCreate, done) {
      const docs = [];
      for (let ix = 0; ix < numDocs; ++ix) {
        docs.push(fnCreate());
      }

      PagingDocModel.insertMany(docs, function (err, inserted) {
        if (err) return done(err);

        return done(null, inserted);
      });
    }

    function createDoc() {
      return new PagingDocModel({
        valueA: util.format('DocModel_%s', randomId()),
        valueB: randomId(),
      });
    }


    function withQueryParams(createLogicalQuery, createSortKey) {

      it('query first page of results when only one page exists.', function (done) {
        const numDocs = 10;
        const pgLimit = 10;
        let logicalQuery;
        let sortKey;

        async.waterfall([
          function (callback) {
            suiteSetup(numDocs, createDoc, callback);
          },
          function (docs, callback) {
            logicalQuery = createLogicalQuery(docs);
            sortKey = createSortKey();

            // Query docs.
            PagingDocModel.findWithPaging(
              logicalQuery,
              {},     // all fields
              sortKey,
              pgLimit,
              null,
              function (err, pagedResults, pgNextPageArgs) {
                if (err) return callback(err);

                expect(pagedResults).to.not.equal(null);
                expect(pagedResults.length).to.be.at.most(pgLimit);

                const lastResult = pagedResults[pagedResults.length - 1];
                const testQueryNext = {
                  _id: lastResult._id,
                  valueA: lastResult.valueA,
                };

                expect(pgNextPageArgs).to.deep.equal(testQueryNext);

                return callback(err);
              });
          },
        ], function (err) {
          return done(err);
        });
      });

      it('query first page of results when multiple pages exist.', function (done) {
        const numDocs = 17;    // Not a multiple of pgLimit.
        const pgLimit = 10;
        let logicalQuery;
        let sortKey;

        async.waterfall([
          function (callback) {
            suiteSetup(numDocs, createDoc, callback);
          },
          function (docs, callback) {
            logicalQuery = createLogicalQuery(docs);
            sortKey = createSortKey();

            // Query docs.
            PagingDocModel.findWithPaging(
              logicalQuery,
              {},     // all fields
              sortKey,
              pgLimit,
              null,
              function (err, pagedResults, pgNextPageArgs) {
                if (err) return callback(err);

                expect(pagedResults).to.not.equal(null);
                expect(pagedResults.length).to.be.at.most(pgLimit);

                const lastResult = pagedResults[pagedResults.length - 1];
                const testQueryNext = {
                  _id: lastResult._id,
                  valueA: lastResult.valueA,
                };

                expect(pgNextPageArgs).to.deep.equal(testQueryNext);

                return callback(err);
              });
          },
        ], function (err) {
          return done(err);
        });
      });

      it('query next page of results when multiple pages exist.', function (done) {
        const numDocs = 17;    // Not a multiple of pgLimit
        const pgLimit = 10;
        let logicalQuery;
        let sortKey;

        async.waterfall([
          function (callback) {
            suiteSetup(numDocs, createDoc, callback);
          },
          function (docs, callback) {
            logicalQuery = createLogicalQuery(docs);
            sortKey = createSortKey();

            // Query page 1 of docs.
            PagingDocModel.findWithPaging(
              logicalQuery,
              {},     // all fields
              sortKey,
              pgLimit,
              null,
              function (err, pagedResults, pgNextPageArgs) {
                if (err) return callback(err);

                return callback(null, pagedResults, pgNextPageArgs, docs);
              });
          },
          function (lastResults, pgNextPageArgs, docs, callback) {
            // Query page 2 of docs.
            const pgQueryNext = {
              $or: [
                { valueA: { $gt: pgNextPageArgs.valueA } },
                {
                  $and: [
                    { valueA: pgNextPageArgs.valueA },
                    { _id: { $gt: pgNextPageArgs._id } }
                  ]
                }
              ]
            };

            PagingDocModel.findWithPaging(
              logicalQuery,
              {},     // all fields
              sortKey,
              pgLimit,
              pgQueryNext,
              function (err, pagedResults, pgNextPageArgs) {
                if (err) return callback(err);

                expect(pagedResults).to.not.equal(null);
                expect(pagedResults.length).to.equal(numDocs % pgLimit);
                expect(pagedResults[0]).to.not.deep.equal(lastResults[0]);
                expect(pagedResults[pagedResults.length - 1]).to.not.deep.equal(lastResults[lastResults.length - 1]);

                const lastResult = pagedResults[pagedResults.length - 1];
                const testQueryNext = {
                  _id: lastResult._id,
                  valueA: lastResult.valueA,
                };

                expect(pgNextPageArgs).to.deep.equal(testQueryNext);

                return callback(err);
              });
          },
        ], function (err) {
          return done(err);
        });
      });

      it('query all pages of results when multiple pages exist.', function (done) {
        const numDocs = 100;
        const pgLimit = 7;
        let logicalQuery;
        let sortKey;

        async.waterfall([
          function (callback) {
            suiteSetup(numDocs, createDoc, callback);
          },
          function (docs, callback) {
            logicalQuery = createLogicalQuery(docs);
            sortKey = createSortKey();

            // Query all pages of docs.
            let pgQueryNext = null;

            let pagedDocs = [];
            let keepPaging = true;

            async.whilst(() => keepPaging, function (cbWhilst) {
              PagingDocModel.findWithPaging(
                logicalQuery,
                {},     // all fields
                sortKey,
                pgLimit,
                pgQueryNext,
                function (err, pagedResults, pgNextPageArgs) {
                  if (err) return callback(err);

                  if (pagedResults.length === 0) {
                    // Done
                    keepPaging = false;
                  }
                  else {
                    // Args for next page
                    pgQueryNext = {
                      $or: [
                        { valueA: { $gt: pgNextPageArgs.valueA } },
                        {
                          $and: [
                            { valueA: pgNextPageArgs.valueA },
                            { _id: { $gt: pgNextPageArgs._id } }
                          ]
                        }
                      ]
                    };

                    pagedDocs = pagedDocs.concat(pagedResults);
                  }

                  return cbWhilst(null);
                });
            }, function (err) {
              return callback(err, pagedDocs);
            });
          },
          function (pagedDocs, callback) {
            // Now query with no paging at all (i.e., get all at once.)
            PagingDocModel.findWithPaging(
              logicalQuery,
              {},     // all fields
              sortKey,
              null,   // no limit
              null,
              function (err, pagedResults) {
                if (err) return callback(err);

                // Expect the same results in both arrays.
                expect(pagedResults).to.not.equal(null);
                expect(pagedResults.length).to.equal(pagedDocs.length);

                for (let ix = 0; ix < pagedResults.length; ++ix) {
                  expect(pagedResults[ix]._id.toString()).to.equal(pagedDocs[ix]._id.toString());
                }

                return callback(null);
              });
          }
        ], function (err) {
          return done(err);
        });
      });
    }

    const params = [
      [
        () => { return {}; },   // All docs
        () => { return { valueA: 1, _id: 1 }; }
      ],
      [
        (docs) => {
          return {
            _id: { $in: docs.map(doc => doc._id) }    // Redundant: still all docs, but does affect the query logic.
          };
        },
        () => { return { valueA: 1, _id: 1 }; }
      ]
    ];

    params.forEach(paramPair => {
      const createLogicalQuery = paramPair[0];
      const createSortKey = paramPair[1];

      describe(`with logical query: ${createLogicalQuery} and sortKey: ${createSortKey}`, function () {
        withQueryParams(createLogicalQuery, createSortKey);
      });
    });


  });
});



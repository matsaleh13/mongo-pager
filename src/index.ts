import {Collection} from "mongodb";
import * as _ from "lodash";

/**
 * Generic function for querying a MongoDB collection with paging support.
 *
 * Paging breaks the results of a query into multiple chunks or "pages".
 * Each page is retrived by calling this function with query parameters
 * that identify the starting point of the page in the overall results
 * and the number of documents to return in that page.
 *
 * The results are sorted according to criteria specified in a sortKey object
 * that identifies fields of the collection to be used as sort keys.
 * For best performance, the collection should have an index that
 * uses the key fields identified by the sortKey object.
 *
 * The first "page" of results is retrieved by passing null to the pgQuery
 * parameter. Subsequent "pages" are retrieved by passing the object
 * passed to the pgNextPageArgs parameter of the callback from the previous call.
 *
 * @param {Object} [collection] The MongoDB collection object representing the collection to be queried.
 * @param {Object} [query] JS object containing the logical query criteria. These criteria define the intended complete result.
 * @param {Object} [projection] JS object identifying the fields to be included or excluded from the results. Pass empty object ({}) for all.
 * @param {Object} [sortKey] JS object identifying the field(s) that define the sort key for the results. Pass empty object ({}) for natural order.
 * @param {Number} [pgLimit] The max number of doc results in a page. Pass null to retrieve all results. Not recommended for large result sets.
 * @param {Number} [pgQuery] JS object containing paging query criteria. These define the starting point of each page of the results. Pass null to retrieve the first page.
 * @param {Function} [done] Completion callback. Params: (err, pagedResults, pgNextPageArgs). If pagedResults.length == 0, pgNextPageArgs is undefined.
 */
function _findWithPaging(collection:Collection, query:Object, projection:Object, sortKey:Object, pgLimit:number, pgQuery:Object, done:Function) {
  // Logical query parameters.
  const queryParams = {
    $and: [
      _.clone(query)
    ]
  };
  // console.info('queryParams => %s', util.inspect(queryParams, false, null));

  // Additional query params for paging support.
  // The criteria here must specify the starting document of a results page.
  if (pgQuery !== null) {
    queryParams.$and.push(_.clone(pgQuery));
  }
  // console.info('queryParams => %s', util.inspect(queryParams, false, null));

  collection.find(queryParams)
    .project(projection)
    .limit(pgLimit)
    .hint(sortKey)
    .sort(sortKey)
    .toArray(function (err:Error, pagedResults:Array<any>) {
      if (err) return done(err);

      const pgNextPageArgs:any = {};

      // Get the values for the next page's query from the last result's
      // fields that match the keys of the sortKey.
      if (pagedResults.length > 0) {
        const lastResult = pagedResults[pagedResults.length - 1];

        const keys = _.keys(sortKey);
        if (keys.length === 0) {
          // Sort key had nothing, assume natural sort.
          pgNextPageArgs._id = lastResult._id;
        }
        else {
          // Use sortKey fields to identify fields to return for next page query.
          keys.forEach(key => {
            pgNextPageArgs.key = lastResult[key];
          });
        }
      }

      return done(null, pagedResults, pgNextPageArgs);
    });
}


/**
 * Custom error type.
 */
export class PagingQueryError extends Error {
  constructor(message:string) {
    super(message);

    this.name = 'PagingQueryError';
  }
}

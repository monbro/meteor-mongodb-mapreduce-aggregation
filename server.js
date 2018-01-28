const tl = TLog ? TLog.getLogger() : undefined;
// hacky advanced mongo definitions based on https://github.com/meteor/meteor/pull/644

import Future from 'fibers/future';

const dummyCollection = new Meteor.Collection('__dummy__');

// Wrapper of the call to the db into a Future
const futureWrapper = (collection, commandName, args) => {
  const col = (typeof collection) === 'string' ? dummyCollection : collection;
  const collectionName = (typeof collection) === 'string' ? collection : collection._name;

  const coll1 = col.find()._mongo.db.collection(collectionName);

  const future = new Future;
  const cb = future.resolver();
  args = args.slice();
  args.push(cb);
  coll1[commandName].apply(coll1, args);
  return future.wait();
};



// Not really DRY, but have to return slightly different results from mapReduce as mongo method returns
// a mongo collection, which we don't need here at all
const callMapReduce = (collection, map, reduce, options) => {
  const col = (typeof collection) === 'string' ? dummyCollection : collection;
  const collectionName = (typeof collection) === 'string' ? collection : collection._name;

  if (tl) {
    tl.debug(`callMapReduce called for collection ${collectionName} map: ${map} reduce: ${reduce}${` options: ${JSON.stringify(options)}`}`);
  }

  const coll1 = col.find()._mongo.db.collection(collectionName);

  const future = new Future;
  coll1.mapReduce(map, reduce, options, (err, result, stats) => {
    if (err) { future.throw(err); }
    const res = { collectionName: result.collectionName, stats };
    return future.return([true, res]);
  });

  const result = future.wait();
  if (!result[0]) { throw result[1]; }
  return result[1];
};

// Extending Collection on the server
Meteor.Collection.prototype.distinct = function (key, query, options) {
  return futureWrapper(this._name, 'distinct', [key, query, options]);
};

Meteor.Collection.prototype.aggregate = function (pipeline) {
  return futureWrapper(this._name, 'aggregate', [pipeline]);
};

Meteor.Collection.prototype.mapReduce = function (map, reduce, options) {
  options = options || {};
  options.readPreference = 'primary';
  return callMapReduce(this._name, map, reduce, options);
};

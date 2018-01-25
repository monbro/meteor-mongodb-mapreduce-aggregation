meteor-mongodb-mapreduce-aggregation
====================================

Very simple implementation of some of mongodb aggregation framework functions for Meteor.

**meteor-mongodb-mapreduce-aggregation** is a fork of [meteor-mongo-server](https://github.com/zvictor/meteor-mongo-server)
that do not expose the aggregation framework to the client, being available only on server side.

Unfortunately the original source was not working for me anymore as a plugin and seems to be not maintained. So I created this plugin and bugfixed it.

It extends `Collection` with 3 methods so far, **mapReduce**, **distinct** and **aggregate**, so that you can do:

```javascript
    const col = new Meteor.Collection("name");

    if (Meteor.isServer) {
      // mapReduce
      const map = () => emit(this.Region, this.Amount);
      const reduce = (reg, am) => Array.sum(am);

      col.mapReduce(map, reduce, {out: "out_collection_name", verbose: true}, (err, res) => console.dir(res.stats)); // statistics object for running mapReduce

      // distinct
      let result = col.distinct("Field Name");
      console.dir(result);

      //aggregate
      result = col.aggregate(pipeline);
      console.dir(result);
    }
```

Another [mapReduce](http://docs.mongodb.org/manual/core/map-reduce/) example in javascript:
```javascript

    // on the server side
    /////////////////////

    Posts = new Mongo.Collection("Posts");
    Tags = new Mongo.Collection("Tags");

    Meteor.methods({

        mostUsedTags: function() {
            var map = function() {
                if (!this.tags) {
                    return;
                }

                for (index in this.tags) {
                    emit(this.tags[index], 1);
                }
            }

            var reduce = function(previous, current) {
                var count = 0;

                for (index in current) {
                    count += current[index];
                }

                return count;
            }

            // keep in mind that executing the mapReduce function will override every time the collection Tags
            var result = Posts.mapReduce(map, reduce, {query: {}, out: "Tags", verbose: true});

            // now return all the tags, ordered by usage
            // as an alternative solution you can also publish the collection Tags and use this one at the client side
            return Tags.find({},{ sort: {'value': -1}, limit:10}).fetch();
        }

    });

    // on the client side you could do
    //////////////////////////////////

    Meteor.call("mostUsedTags", function(err, data) {
        console.log(data);
    });

```

To install it, run:
```bash
$ meteor add monbro:mongodb-mapreduce-aggregation
```

This package is MIT Licensed. Do whatever you like with it but any responsibility for doing so is your own.

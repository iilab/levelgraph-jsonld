var jsonld = require('jsonld'),
    uuid   = require('uuid'),
    RDFTYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    RDFLANGSTRING = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString',
    XSDTYPE = 'http://www.w3.org/2001/XMLSchema#',
    async = require('async'),
    N3Util = require('n3/lib/N3Util'); // with browserify require('n3').Util would bundle more then needed!

function levelgraphJSONLD(db, jsonldOpts) {

  if (db.jsonld) {
    return db;
  }

  var graphdb = Object.create(db);

  jsonldOpts = jsonldOpts || {};
  jsonldOpts.base = jsonldOpts.base || '';

  graphdb.jsonld = {
      options: jsonldOpts
  };

  // Post might follow HTTP POST semantics as doPut used to by returning "missing" ids for blank nodes
  // (i.e. interpreting blank nodes as nested Posts).

  // Put might then focus on existing resources and return information about conflicts when they occur

  // Patch would then follow LD Patch, while we might have an Update to deal with graphs (and follow SPARQL Update)
  // Path might be its own level-jsonld-patch module.

  //

  function doPut(obj, options, callback) {
    var blanks = {};

    if (options.base) {
      if (obj['@context']) {
        obj['@context']['@base'] = options.base;
      } else {
        obj['@context'] = { '@base' : options.base };
      }
    }
    // console.time('doPut expand')

    jsonld.expand(obj, function(err, expanded) {
      // console.timeEnd('doPut expand')
      if (err) {
        return callback && callback(err);
      }
      // console.log("expanded", expanded)
      // console.time('doPut toRDF')

      jsonld.toRDF(expanded, options, function(err, triples) {
        // console.timeEnd('doPut toRDF')

        if (err || triples.length === 0) {
          return callback && callback(err, null);
        }

        var stream = graphdb.putStream();
        // console.time('doPut stream')
        // console.time('doPut stream opening')
        var opening = true;
        // stream.on('data', function() { opening && console.timeEnd('doPut stream opening'); opening = false; });
        // stream.on('finish', function() { console.timeEnd('doPut stream finishing'); });
        stream.on('error', callback);
        stream.on('close', function() {
          if (options.blank_ids) {
            // return rdf store scoped blank nodes

            var blank_keys = Object.keys(blanks);
            var clone_obj = Object.assign({}, obj)
            var frame;
            frame = (function framify(o) {
              Object.keys(o).map(function(key) {
                if (Array.isArray(o[key]) && key != "@type") {
                  o[key] = o[key][0];
                } else if (typeof o[key] === "object") {
                  o[key] = framify(o[key]);
                }
              })
              return o;
            })(clone_obj)

            if (blank_keys.length != 0) {
              jsonld.frame(obj, frame, function(err, framed) {
                if (err) {
                  return callback(err, null);
                }
                var framed_string = JSON.stringify(framed);

                blank_keys.forEach(function(blank) {
                  framed_string = framed_string.replace(blank,blanks[blank])
                })
                var ided = JSON.parse(framed_string);
                if (ided["@graph"].length == 1) {
                  var clean_reframe = Object.assign({}, { "@context": ided["@context"]}, ided["@graph"][0]);
                  return callback(null, clean_reframe);
                } else if (ided["@graph"].length > 1) {
                  return callback(null, ided);
                } else {
                  // Could not reframe the input, returning the original object
                  return callback(null, obj);
                }
              })
            } else {
              return callback(null, obj);
            }
        });

        console.time('doPut graph')

        async.eachSeries(Object.keys(triples), function(graph_key, cb) {
          var graph_name;

          var store_keys;
          if (graph_key === '@default') {
            // empty graph is @default for now.
            store_keys = ['subject', 'predicate', 'object'];
          } else {
            store_keys = ['subject', 'predicate', 'object', 'graph'];
          }

          // console.time('doPut list')

          var list = triples[graph_key].map(function(triple) {
            // console.log(triple)
            // console.time('doPut' + triple.subject.value)

            var ret = store_keys.reduce(function(acc, key) {
              if(key === 'graph') {
                acc[key] = graph_key;
              } else {
                var node = triple[key];
                // generate UUID to identify blank nodes
                // uses type field set to 'blank node' by jsonld.js toRDF()
                if (node.type === 'blank node') {
                  if (!blanks[node.value]) {
                    blanks[node.value] = '_:' + uuid.v1();
                  }
                  node.value = blanks[node.value];
                }
                // preserve object data types using double quotation for literals
                // and don't keep data type for strings without defined language
                if(key === 'object' && triple.object.datatype){
                  if(triple.object.datatype.indexOf(XSDTYPE) != -1){
                    if(triple.object.datatype === 'http://www.w3.org/2001/XMLSchema#string'){
                      node.value = '"' + triple.object.value + '"';
                    } else {
                      node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                    }
                  } else if(triple.object.datatype.indexOf(RDFLANGSTRING) != -1){
                    node.value = '"' + triple.object.value + '"@' + triple.object.language;
                  } else {
                    node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                  }
                }
                acc[key] = node.value;
              }
              return acc;
            }, {});
            // console.timeEnd('doPut' + triple.subject.value)

            return ret;
          })

          // console.timeEnd('doPut list')
          // console.log("list", JSON.stringify(list,true,2))
          async.eachSeries(list, function(triple, cb) {
            stream.write(triple, cb);
            // console.timeEnd('doPut' + triple.subject)
            // (function write(triple, done) {
            //    var ret = stream.write(triple);
            //    if (ret) {
            //      cb();
            //    } else {
            //      stream.once('drain', write(triple,cb));
            //    }
            // })(triple);
          }, cb);
        }, function(err) {
          // console.timeEnd('doPut graph')
          // console.time('doPut stream closing')
          // console.time('doPut stream finishing')
          stream.end();
        });
      });
    });
  }


  function doPutSync(obj, options, callback) {
    var blanks = {};

    if (options.base) {
      if (obj['@context']) {
        obj['@context']['@base'] = options.base;
      } else {
        obj['@context'] = { '@base' : options.base };
      }
    }
    // console.time('doPut expand')

    jsonld.expand(obj, function(err, expanded) {
      // console.timeEnd('doPut expand')
      if (err) {
        return callback && callback(err);
      }

      // console.time('doPut toRDF')

      jsonld.toRDF(expanded, options, function(err, triples) {
        // console.timeEnd('doPut toRDF')

        if (err || triples.length === 0) {
          return callback && callback(err, null);
        }

        var lists = Object.keys(triples).reduce(function(acc, graph_key) {
          var graph_name;

          var store_keys;
          if (graph_key === '@default') {
            // empty graph is @default for now.
            store_keys = ['subject', 'predicate', 'object'];
          } else {
            store_keys = ['subject', 'predicate', 'object', 'graph'];
          }

          // console.time('doPut list')

          var list = triples[graph_key].map(function(triple) {
            // console.log(triple)
            // console.time('doPut' + triple.subject.value)

            var ret = store_keys.reduce(function(acc, key) {
              if(key === 'graph') {
                acc[key] = graph_key;
              } else {
                var node = triple[key];
                // generate UUID to identify blank nodes
                // uses type field set to 'blank node' by jsonld.js toRDF()
                if (node.type === 'blank node') {
                  if (!blanks[node.value]) {
                    blanks[node.value] = '_:' + uuid.v1();
                  }
                  node.value = blanks[node.value];
                }
                // preserve object data types using double quotation for literals
                // and don't keep data type for strings without defined language
                if(key === 'object' && triple.object.datatype){
                  if(triple.object.datatype.indexOf(XSDTYPE) != -1){
                    if(triple.object.datatype === 'http://www.w3.org/2001/XMLSchema#string'){
                      node.value = '"' + triple.object.value + '"';
                    } else {
                      node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                    }
                  } else if(triple.object.datatype.indexOf(RDFLANGSTRING) != -1){
                    node.value = '"' + triple.object.value + '"@' + triple.object.language;
                  } else {
                    node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                  }
                }
                acc[key] = node.value;
              }
              return acc;
            }, {});
            // console.timeEnd('doPut' + triple.subject.value)

            return ret;
          })

          // console.timeEnd('doPut list')
          return acc.concat(list)
        }, []);


        graphdb.put(lists, /*{sync: true},*/ function(err, ret) {
          // console.log("sync")
          if (err) {
            callback(err, null);
          }
          var blank_keys = Object.keys(blanks);
          var clone_obj = Object.assign({}, obj)
          var frame;
          frame = (function framify(o) {
            Object.keys(o).map(function(key) {
              if (Array.isArray(o[key]) && key != "@type") {
                o[key] = o[key][0];
              } else if (o[key] && typeof o[key] === "object") {
                o[key] = framify(o[key]);
              }
            })
            return o;
          })(clone_obj)

          if (blank_keys.length != 0) {
            jsonld.frame(obj, frame, function(err, framed) {
              if (err) {
                callback(err, null);
              }
              var framed_string = JSON.stringify(framed);

              blank_keys.forEach(function(blank) {
                framed_string = framed_string.replace(blank,blanks[blank])
              })
              var ided = JSON.parse(framed_string);
              // console.timeEnd('doPut close')
              if (ided["@graph"].length == 1) {
                var clean_reframe = Object.assign({}, { "@context": ided["@context"]}, ided["@graph"][0]);
                callback(null, clean_reframe);
              } else if (ided["@graph"].length > 1) {
                callback(null, ided);
              } else {
                // Could not reframe the input, returning the original object
                callback(null, obj);
              }
            })
>>>>>>> Working on locking
          } else {
            // console.timeEnd('doPut close')
            // console.log("callback", obj["@graph"]["@id"])
            callback(null, obj);
          }
        });


      });
    });
  }

  // Put needs to check if existing triples match the submitted triple before inserting them.
  // Matching needs to be defined, for instance:
  //   - Unicity of a property: s p o / s p o'
  //   - Negation of an existing Asserted triple. s p o a / s p o n
  //   -
  // passing a `check` callback function could be a good way to do this.


  // The below approach runs into an atomicity problem as the stream writes but
  // the db.get doesn't catch up fast enough. So either:
  //   - I find a way to flush the stream and ensure atomicity after the stream ends.
  //   - I look into using level-hooks to implement the check.

  function doCheck(obj, options, checkfn, callback) {
    var blanks = {};
    var conflicts = [];

    if (options.base) {
      if (obj['@context']) {
        obj['@context']['@base'] = options.base;
      } else {
        obj['@context'] = { '@base' : options.base };
      }
    }
    jsonld.expand(obj, function(err, expanded) {
      if (err) {
        return callback && callback(err);
      }

      // console.log("expanded", JSON.stringify(expanded,true,2))

      jsonld.toRDF(expanded, options, function(err, triples) {

        // console.log("triples", JSON.stringify(triples,true,2))

        if (err || triples.length === 0) {
          return callback(err, null, fails);
        }

        var ret = {};

        async.each(Object.keys(triples), function(graph_key, cbGraph) {
          var graph_name;

          var store_keys;
          if (graph_key === '@default') {
            // Do empty graph is @default for now.
            store_keys = ['subject', 'predicate', 'object'];
          } else {
            store_keys = ['subject', 'predicate', 'object', 'graph'];
          }

          async.reduce(triples[graph_key].map(function(triple) {

            return store_keys.reduce(function(acc, key) {
              if(key === 'graph') {
                acc[key] = graph_key;
              } else {
                var node = triple[key];
                // generate UUID to identify blank nodes
                // uses type field set to 'blank node' by jsonld.js toRDF()
                if (node.type === 'blank node') {
                  if (!blanks[node.value]) {
                    blanks[node.value] = '_:' + uuid.v1();
                  }
                  node.value = blanks[node.value];
                }
                // preserve object data types using double quotation for literals
                // and don't keep data type for strings without defined language
                if(key === 'object' && triple.object.datatype){
                  if(triple.object.datatype.match(XSDTYPE)){
                    if(triple.object.datatype === 'http://www.w3.org/2001/XMLSchema#string'){
                      // return strings as simple JSON values to match input
                      node.value = triple.object.value;
                    } else {
                      node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                    }
                  } else if(triple.object.datatype.match(RDFLANGSTRING)){
                    node.value = '"' + triple.object.value + '"@' + triple.object.language;
                  } else {
                    node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                  }
                }
                acc[key] = node.value;
              }
              return acc;
            }, {});
          }), {}, function(ret, triple, cb) {
            // console.log("triple", triple)
            var checked = checkfn(triple);
            if (checked === true) {
              ret[triple.subject] = ret[triple.subject] ? ret[triple.subject] : { '@id': triple.subject };
              if (Array.isArray(ret[triple.subject][triple.predicate])) {
                ret[triple.subject][triple.predicate].push(triple.object);
              } else {
                ret[triple.subject][triple.predicate] = [triple.object];
              }
              cb(null, ret);
            } else if (typeof checked === 'object') {
              ret[triple.subject] = ret[triple.subject] ? ret[triple.subject] : { '@id': triple.subject };
              graphdb.get(checked, function(err, results) {
                // console.log("err", err)
                if (err) cb(err)
                // console.log("dynamic check : ", checked)
                // console.log("validated triple: " + JSON.stringify(triple,true,2))
                // console.log("results : ", results)
                if (results.length == 0) {
                  if (Array.isArray(ret[triple.subject][triple.predicate])) {
                    ret[triple.subject][triple.predicate].push(triple.object);
                  } else {
                    ret[triple.subject][triple.predicate] = [triple.object];
                  }
                  // console.log("ret", ret)
                  cb(null, ret);
                } else {
                  // console.log("conflict", triple)
                  conflicts.push(triple);
                  cb(null, ret);
                }
              });
            } else {
              // console.log("fails doPut check: " + result)
              // console.log("failed triple: " + JSON.stringify(triple,true,2))
              // console.log("conflict", triple)
              conflicts.push(triple);
              cb(null, ret);
            }
          }, function(err, result) {
            if (err) cbGraph(err)
            // console.log("result", result)
            ret[graph_key] = result
            cbGraph()
          });
        }, function(err) {
          if (err) callback(err,null)
          // TODO: Fix the problem with framing graphs. Named graph key values aren't returned at the root
          // as they are not processed correctly by the JSON-LD framing algorithm
          // console.log("ret", ret)
          var checked = Object.keys(ret).reduce(function(obj, graph) {
            if (graph == "@default") {
              return Object.assign(obj, { "content": { "@graph": Object.keys(ret[graph]).map(function(i) { return ret[graph][i] }) }})
            } else if (ret[graph][Object.keys(ret[graph])[0]]) {
              return Object.assign(obj, { "graph": ret[graph][Object.keys(ret[graph])[0]] } )
            }
          }, {})
          // console.log("checked['content']", checked["content"])
          // console.log("checked['graph']", checked["graph"])
          callback(null, conflicts, checked["content"], checked["graph"])
        });
      });
    });
  }

  function doDel(obj, options, callback) {
    var blanks = {};
    jsonld.expand(obj, options, function(err, expanded) {
      if (err) {
        return callback && callback(err);
      }

      var stream  = graphdb.delStream();
      stream.on('close', callback);
      stream.on('error', callback);

      if (options.base) {
        if (expanded['@context']) {
          expanded['@context']['@base'] = options.base;
        } else {
          expanded['@context'] = { '@base' : options.base };
        }
      }

      jsonld.toRDF(expanded, options, function(err, triples) {
        if (err || triples.length === 0) {
          return callback(err, null);
        }

        triples['@default'].map(function(triple) {

          return ['subject', 'predicate', 'object'].reduce(function(acc, key) {
            var node = triple[key];
            // mark blank nodes to skip deletion as per https://www.w3.org/TR/ldpatch/#Delete-statement
            // uses type field set to 'blank node' by jsonld.js toRDF()
            if (node.type === 'blank node') {
              if (!blanks[node.value]) {
                blanks[node.value] = '_:';
              }
              node.value = blanks[node.value];
            }
            // preserve object data types using double quotation for literals
            // and don't keep data type for strings without defined language
            if(key === 'object' && triple.object.datatype){
              if(triple.object.datatype.match(XSDTYPE)){
                if(triple.object.datatype === 'http://www.w3.org/2001/XMLSchema#string'){
                  node.value = '"' + triple.object.value + '"';
                } else {
                  node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                }
              } else if(triple.object.datatype.match(RDFLANGSTRING)){
                node.value = '"' + triple.object.value + '"@' + triple.object.language;
              } else {
                node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
              }
            }
            acc[key] = node.value;
            return acc;
          }, {});
        }).forEach(function(triple) {
          // Skip marked blank nodes.
          if (triple.subject.indexOf('_:') !== 0 && triple.object.indexOf('_:') !== 0) {
            stream.write(triple);
          }
        });
        stream.end();
      });
    })
  }

  function doCut(obj, options, callback) {
    var iri = obj;
    if (typeof obj !=='string') {
      iri = obj['@id'];
    }
    if (iri === undefined) {
      return callback && callback(null);
    }

    var stream = graphdb.delStream();
    stream.on('close', callback);
    stream.on('error', callback);

    (function delAllTriples(iri, done) {
      graphdb.get({ subject: iri }, function(err, triples) {
        async.each(triples, function(triple, cb) {
          stream.write(triple);
          if (triple.object.indexOf('_:') === 0 || (options.recurse && N3Util.isIRI(triple.object))) {
            delAllTriples(triple.object, cb);
          } else {
            cb();
          }
        }, done);
      });
    })(iri, function(err) {
      if (err) {
        return callback(err);
      }
      stream.end();
    });
  }

  graphdb.jsonld.put = function(obj, options, callback) {

    if (typeof obj === 'string') {
      obj = JSON.parse(obj);
    }

    if (typeof options === 'function') {
      callback = options;
      options = {};
    }

    options.base = ( obj["@context"] && obj["@context"]["@base"] ) || options.base || this.options.base;
    options.overwrite = options.overwrite !== undefined ? options.overwrite : ( this.options.overwrite !== undefined ? this.options.overwrite : false );
    if (!options.overwrite) {
      if (options.sync) {
        doPutSync(obj, options, callback);
      } else {
        doPut(obj, options, callback);
      }
    } else {
      graphdb.jsonld.del(obj, options, function(err) {
        if (err) {
          return callback && callback(err);
        }
      });
      doPut(obj, options, callback);
    }
  };

  graphdb.jsonld.post = function(obj, options, callback) {

    if (typeof obj === 'string') {
      obj = JSON.parse(obj);
    }

    if (typeof options === 'function') {
      callback = options;
      options = {};
    }

    options.base = ( obj["@context"] && obj["@context"]["@base"] ) || options.base || this.options.base;
    options.overwrite = options.overwrite !== undefined ? options.overwrite : ( this.options.overwrite !== undefined ? this.options.overwrite : false );

    if (!options.overwrite) {
      doPost(obj, options, callback);
    } else {
      graphdb.jsonld.del(obj, options, function(err) {
        if (err) {
          return callback && callback(err);
        }
      });
      doPost(obj, options, callback);
    }
  };

  // Gets an object to Put and a check function which takes triples and returns a boolean.
  // calls the callback function with a list of conflicting triples and a JSON-LD document without conflicts.
  // That's a leaky abtraction (since we bring up triples in a JSON-LD world) and to address this we would ideally want
  // a validation language (RDFS, OWL) to be used instead of a check function.

  graphdb.jsonld.check = function(obj, options, checkfn, callback) {

    if (typeof obj === 'string') {
      obj = JSON.parse(obj);
    }

    if (callback === undefined && typeof checkfn == 'function') {
      callback = checkfn;
      checkfn = options;
      options = {};
    } else if (checkfn === undefined && callback === undefined) {
      callback = options;
      checkfn = function() { return true };
      options = {};
    }

    options.base = ( obj["@context"] && obj["@context"]["@base"] ) || options.base || this.options.base;
    options.overwrite = options.overwrite !== undefined ? options.overwrite : ( this.options.overwrite !== undefined ? this.options.overwrite : false );

    var frame = { "@context": obj["@context"] };

    // console.log("frame")
    // console.log(JSON.stringify(frame,true,2))
    jsonld.compact(frame, {}, function(err, compact_frame) {
      if (err || compact_frame === null) {
        return callback(err, compact_frame);
      }
      // console.log("compact_frame")
      // console.log(JSON.stringify(compact_frame,true,2))

      jsonld.compact(obj, {}, function(err, compacted) {
        if (err) {
          return callback && callback(err);
        }
        // console.log("compacted")
        // console.log(JSON.stringify(compacted,true,2))
        doCheck(compacted, options, checkfn, function(err, conflicts, checked, graph) {
          if (err || checked === null) {
            return callback(err, checked);
          }
          // console.log("checked")
          // console.log(JSON.stringify(checked,true,2))
          // console.log("conflicts")
          // console.log(JSON.stringify(conflicts,true,2))

          jsonld.frame(checked, frame, function(err, framed) {
            if (err || framed === null) {
              return callback(err, framed);
            }
            // console.log("framed")
            // console.log(JSON.stringify(framed,true,2))
            var context = frame["@context"] || {};

            jsonld.compact(framed, context, options, function(err, compacted) {
              // console.log("compacted")
              // console.log(JSON.stringify(compacted,true,2))
              var processed = graph ? Object.assign(compacted, { "@graph": graph } ) : compacted;
              callback(null, conflicts, processed);
            });
          });
        });
      });
    })
  };

  graphdb.jsonld.del = function(obj, options, callback) {

    if (typeof options === 'function') {
      callback = options;
      options = {};
    }

    options.cut = options.cut !== undefined ? options.cut : ( this.options.cut !== undefined ? this.options.cut : false );
    options.recurse = options.recurse !== undefined ? options.recurse : ( this.options.recurse !== undefined ? this.options.recurse : false );

    if (typeof obj === 'string') {
      try {
        obj = JSON.parse(obj);
      } catch (e) {
        if (typeof obj !== 'string' || ( N3Util.isIRI(obj) && !options.cut )) {
          callback(new Error("Passing an IRI to del is not supported anymore. Please pass a JSON-LD document."))
        }
      }
    }

    if (!options.cut) {
      doDel(obj, options, callback)
    } else {
      doCut(obj, options, callback)
    }
  };

  graphdb.jsonld.cut = function(obj, options, callback) {

    if (typeof options === 'function') {
      callback = options;
      options = {};
    }

    options.recurse = options.recurse ||  this.options.recurse || false;

    doCut(obj, options, callback);
  }

  // http://json-ld.org/spec/latest/json-ld-api/#data-round-tripping
  function getCoercedObject(object) {
    var TYPES = {
      PLAIN: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral',
      BOOLEAN: XSDTYPE + 'boolean',
      INTEGER: XSDTYPE + 'integer',
      DOUBLE: XSDTYPE + 'double',
      STRING: XSDTYPE + 'string',
    };
    var value = N3Util.getLiteralValue(object);
    var type = N3Util.getLiteralType(object);
    var coerced = {};
    switch (type) {
      case TYPES.STRING:
      case TYPES.PLAIN:
        coerced['@value'] = value;
        break;
      case RDFLANGSTRING:
        coerced['@value'] = value;
        coerced['@language'] = N3Util.getLiteralLanguage(object);
        break;
      case TYPES.INTEGER:
        coerced['@value'] = parseInt(value, 10);
        break;
      case TYPES.DOUBLE:
        coerced['@value'] = parseFloat(value);
        break;
      case TYPES.BOOLEAN:
        if (value === 'true' || value === '1') {
          coerced['@value'] = true;
        } else if (value === 'false' || value === '0') {
          coerced['@value'] = false;
        } else {
          throw new Error('value not boolean!');
        }
        break;
      default:
        coerced = { '@value': value, '@type': type };
    }
    return coerced;
  }
  var depth;

  function fetchExpandedTriples(iri, frame, callback) {
    depth++;
    var memo = {};
    if (typeof frame === 'function') {
      callback = frame;
      frame = {};
    }
    debugger;
    var dpth = Array(depth).join(">");
    console.log(dpth + "fetchExpandedTriples.iri", iri)
    // console.time(dpth + "fetchExpandedTriples / " + iri)
    function followFrame(triple, frame) {
      return ( frame && frame["@embed"] !== "@never" || frame && frame["@embed"] === undefined )
            || frame === undefined
    }
    graphdb.get({ subject: iri }, function(err, triples) {
      if (err || triples.length === 0) {
        return callback(err, null);
      }
      // console.log("get.triples", triples)
      async.reduce(triples, memo, function(acc, triple, cb) {
        // console.log("reduce.acc", acc)
        var key;

        if (!acc[triple.subject] && !N3Util.isBlank(triple.subject)) {
          acc[triple.subject] = { '@id': triple.subject };
        } else if (N3Util.isBlank(triple.subject) && !acc[triple.subject]) {
          acc[triple.subject] = {};
        }
        if (triple.predicate === RDFTYPE) {
          if (acc[triple.subject]['@type']) {
            acc[triple.subject]['@type'].push(triple.object);
          } else {
            acc[triple.subject]['@type'] = [triple.object];
          }
          return cb(null, acc);
        } else if (!N3Util.isBlank(triple.object)) {
          var object = {};
          if (N3Util.isIRI(triple.object)) {
            object['@id'] = triple.object;
          } else if (N3Util.isLiteral(triple.object)) {
            object = getCoercedObject(triple.object);
          }
          if(object['@id'] && followFrame(triple, frame) ) {
            // expanding object iri
            fetchExpandedTriples(triple.object, frame && frame[triple.predicate], function(err, expanded) {
              if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
              if (expanded !== null) {
                acc[triple.subject][triple.predicate].push(expanded[triple.object]);
              } else {
                acc[triple.subject][triple.predicate].push(object);
              }
              return cb(err, acc);
            });
          } else if (Array.isArray(acc[triple.subject][triple.predicate])){
            acc[triple.subject][triple.predicate].push(object);
            return cb(err, acc);
          } else {
            acc[triple.subject][triple.predicate] = [object];
            return cb(err, acc);
          }
        } else if (followFrame(triple, frame)) {
          // deal with blanks
          fetchExpandedTriples(triple.object, frame && frame[triple.predicate], function(err, expanded) {
            if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
            if (expanded !== null) {
              acc[triple.subject][triple.predicate].push(expanded[triple.object]);
            } else {
              acc[triple.subject][triple.predicate].push(object);
            }
            return cb(err, acc);
          });
        }
      }, function(err, result) {
        console.log(dpth + "callback.iri", iri)
         return callback(err, result);
        // return process.nextTick(callback);
      });
      // console.timeEnd(dpth + "fetchExpandedTriples / " + iri)
      // console.trace();
    });
  }

  graphdb.jsonld.get = function(frame, context, options, callback) {
    var iri;

    if (typeof options === 'function') {
      callback = options;
      options = {};
    } else if (typeof context === 'function') {
      callback = context;
      context = frame["@context"] || {};
      options = {};
    }

    options.base = ( frame["@context"] && frame["@context"]["@base"] ) || ( context && context["@base"] ) || options.base || this.options.base;
    // console.log("iri", iri)

    if (typeof frame === 'string') {
      iri = frame
      frame = {};
      frame["@id"] = N3Util.isIRI(iri) ? iri : options.base + iri;
    }
    // console.log("iri", iri)

    jsonld.compact(frame, {}, function(err, compacted) {
      // console.log("compacted")
      // console.log(JSON.stringify(compacted,true,2))
      // console.log("iri", iri)
      if (err || compacted === null) {
        return callback(err, compacted);
      } else if (Object.keys(compacted).length === 0) {
        compacted = frame;
        iri = N3Util.isIRI(frame["@id"]) ? frame["@id"] : options.base + frame["@id"];
      } else if (compacted['@graph']) {
        iri = compacted['@graph'][0]
              ? compacted['@graph'][0]['@id'].match(options.base) ? compacted['@graph'][0]['@id'] : options.base + compacted['@graph'][0]['@id']
              : compacted["@id"][0].match(options.base) ? compacted["@id"][0] : options.base + compacted["@id"][0]
      } else {
        iri = N3Util.isIRI(compacted["@id"]) ? compacted["@id"] : options.base + compacted["@id"]
      }
      console.log("get.iri", iri)
      // console.log("frame", frame)
      // console.log("context", context)

      depth = 0;
      fetchExpandedTriples(iri, compacted, function(err, expanded) {
        depth = 0;
        if (err || expanded === null) {
          return callback(err, expanded);
        }
        // console.log("expanded")
        // console.log(JSON.stringify(expanded,true,2))
        // console.log("frame")
        // console.log(JSON.stringify(frame,true,2))
        jsonld.frame(expanded, frame, function(err, framed) {
          if (err || framed === null) {
            return callback(err, framed);
          }
          // console.log("framed")
          // console.log(JSON.stringify(framed,true,2))
          jsonld.compact(framed, context, options, callback);
        });
      });
    });
  };

  return graphdb;
}

module.exports = levelgraphJSONLD;

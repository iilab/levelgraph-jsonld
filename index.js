var jsonld = require('jsonld'),
    uuid   = require('uuid'),
    RDFTYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
    RDFLANGSTRING = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString',
    XSDTYPE = 'http://www.w3.org/2001/XMLSchema#',
    async = require('async'),
    debug = require('debug')('levelgraph-jsonld')
    N3Util = require('n3/lib/N3Util'); // with browserify require('n3').Util would bundle more then needed!

require('longjohn')

// framed outputs already included the data in the docunent so we can just delete the blank identifiers.
function unblank(framed) {
  debug('framed', framed)
  var blanks = {}
  var graph = framed['@graph'].filter(function(val) {
    return val['@id'] && val['@id'].indexOf('_:') !== 0
  })

  debug('graph', JSON.stringify(graph,true,2))
  var res = JSON.parse(JSON.stringify(graph, (k,v) => (k === '@id' && v.indexOf('_:') === 0)? undefined : v))

  // TODO: Now replace blank identifier in the document.
  debug('res', JSON.stringify(res,true,2))
  return { '@context': framed['@context'], '@graph': res}
}

function _expandIri(activeCtx, value, relativeTo, localCtx, defined) {
  // ensure value is interpreted as a string
  value = String(value);

  // define term dependency if not defined
  if(localCtx && value in localCtx && defined[value] !== true) {
    _createTermDefinition(activeCtx, localCtx, value, defined);
  }

  relativeTo = relativeTo || {};
  if(relativeTo.vocab) {
    var mapping = activeCtx.mappings[value];

    // value is explicitly ignored with a null mapping
    if(mapping === null) {
      return null;
    }

    if(mapping) {
      // value is a term
      return mapping['@id'];
    }
  }

  // split value into prefix:suffix
  var colon = value.indexOf(':');
  if(colon !== -1) {
    var prefix = value.substr(0, colon);
    var suffix = value.substr(colon + 1);

    // do not expand blank nodes (prefix of '_') or already-absolute
    // IRIs (suffix of '//')
    if(prefix === '_' || suffix.indexOf('//') === 0) {
      return value;
    }

    // prefix dependency not defined, define it
    // if(localCtx && prefix in localCtx) {
    //   _createTermDefinition(activeCtx, localCtx, prefix, defined);
    // }

    // use mapping if prefix is defined
    // var mapping = activeCtx.mappings[prefix];
    // if(mapping) {
    //   return mapping['@id'] + suffix;
    // }

    // already absolute IRI
    return value;
  }

  // prepend vocab
  if(relativeTo.vocab && '@vocab' in activeCtx) {
    return activeCtx['@vocab'] + value;
  }

  // prepend base
  var rval = value;
  if(relativeTo.base) {
    rval = jsonld.prependBase(activeCtx['@base'], rval);
  }

  return rval;
}

function levelgraphJSONLD(db, jsonldOpts) {

  if (db.jsonld) {
    return db;
  }

  var graphdb = Object.create(db);

  jsonldOpts = jsonldOpts || {};
  jsonldOpts.base = jsonldOpts.base || '';

  jsonld.documentLoader = function(url, callback) {
    // noop loader
    debug("documentLoader.url", url)
    return callback();
  };

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
    // debug("obj", JSON.stringify(obj,true,2))
    // console.time('doPut expand')

    jsonld.expand(obj, function(err, expanded) {
      // console.timeEnd('doPut expand')
      // debug("expanded", expanded)
      // debug("err", err)
      if (err) {
        return callback && callback(err);
      }
      // console.time('doPut toRDF')

      jsonld.toRDF(expanded, options, function(err, triples) {
        // console.timeEnd('doPut toRDF')

        if (err || triples.length === 0) {
          return callback && callback(err, null);
        }

        var stream = graphdb.putStream();
        console.time('doPut stream')
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
          } else {
            return callback(null, obj);
          }
        });

        debug('doPut graph triples', triples)

        Object.keys(triples).forEach(function(graph_key) {
          var graph_name;

          var store_keys;
          if (graph_key === '@default') {
            // empty graph is @default for now.
            store_keys = ['subject', 'predicate', 'object'];
          } else {
            store_keys = ['subject', 'predicate', 'object', 'graph'];
          }

          // console.time('doPut list')

          triples[graph_key].map(function(triple) {
            // debug(triple)
            // console.time('doPut' + triple.subject.value)

            var ret = store_keys.reduce(function(acc, key) {
              if(key === 'graph') {
                acc[key] = graph_key;
              } else {
                var node = triple[key];
                // generate UUID to identify blank nodes
                // uses type field set to 'blank node' by jsonld.js toRDF()
                if (node.type === 'blank node') {
                  // debug("triple", triple)
                  // debug("node.value", node.value)
                  // debug("blanks[node.value]", blanks[node.value])
                  if (!blanks[node.value]) {
                    blanks[node.value] = '_:' + uuid.v4();
                  }
                  acc[key] = blanks[node.value];
                  return acc;
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
                    console.log('doPut.triple.object', JSON.stringify(triple.object,true,2))
                    console.log('doPut.createLiteral', N3Util.createLiteral(triple.object.value))
                    // node.value = '"' + triple.object.value + '"^^' + triple.object.datatype;
                    node.value = N3Util.createLiteral(triple.object.value);
                  }
                }
                acc[key] = node.value;
              }
              return acc;
            }, {});
            // console.timeEnd('doPut' + triple.subject.value)

            return ret;
          }).forEach(function(triple) {
            stream.write(triple);
          });
          // return cb();
          // console.timeEnd('doPut list')
          // debug("list", JSON.stringify(list,true,2))
          // async.eachSeries(list, function(triple, _cb) {
          //   stream.write(triple, _cb);
          //   // console.timeEnd('doPut' + triple.subject)
          //   // (function write(triple, done) {
          //   //    var ret = stream.write(triple);
          //   //    if (ret) {
          //   //      cb();
          //   //    } else {
          //   //      stream.once('drain', write(triple,cb));
          //   //    }
          //   // })(triple);
          // }, cb);
        })
        // , function(err) {
          console.timeEnd('doPut stream')
          // console.time('doPut stream closing')
          // console.time('doPut stream finishing')
          stream.end();
        // });
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
            // debug(triple)
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
                    blanks[node.value] = '_:' + uuid.v4();
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
          // debug("sync")
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
          } else {
            // console.timeEnd('doPut close')
            // debug("callback", obj["@graph"]["@id"])
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

      // debug("expanded", JSON.stringify(expanded,true,2))

      jsonld.toRDF(expanded, options, function(err, triples) {

        // debug("triples", JSON.stringify(triples,true,2))

        if (err || triples.length === 0) {
          return callback(err, null, fails);
        }

        var graphs = {};

        async.each(Object.keys(triples), function(graph_key, cbGraph) {
          var graph_name;

          var store_keys;
          if (graph_key === '@default') {
            // Do empty graph is @default for now.
            store_keys = ['subject', 'predicate', 'object'];
          } else {
            store_keys = ['subject', 'predicate', 'object', 'graph'];
          }

          var list = triples[graph_key].map(function(triple) {


            return store_keys.reduce(function(acc, key) {
              if(key === 'graph') {
                acc[key] = graph_key;
              } else {
                var node = triple[key];
                // debug("node",node)
                // generate UUID to identify blank nodes
                // uses type field set to 'blank node' by jsonld.js toRDF()
                if (node.type === 'blank node') {
                  if (!blanks[node.value]) {
                    blanks[node.value] = '_:' + uuid.v4();
                  }
                  node.value = blanks[node.value];
                }
                // preserve object data types using double quotation for literals
                // and don't keep data type for strings without defined language
                if(key === 'object' && triple.object.datatype){
                  if(triple.object.datatype.match(XSDTYPE)){
                    if(triple.object.datatype === 'http://www.w3.org/2001/XMLSchema#string'){
                      // return strings as simple JSON values to match input
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
              }
              // debug("acc", acc)
              return acc;
            }, {});
          })

          async.reduce(list, {}, function(acc, triple, cb) {
            // console.log("triple", triple)
            var checked = checkfn(triple);
            if (checked === true) {

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
              } else if (!N3Util.isBlank(triple.object)) {
                var object = {};
                // debug("triple", triple)
                if (N3Util.isIRI(triple.object)) {
                  // debug("isIRI")
                  object['@id'] = triple.object;
                } else if (N3Util.isLiteral(triple.object)) {
                  object = getCoercedObject(triple.object);
                }
                if(object['@id']) {
                  // expanding object iri
                  if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
                  acc[triple.subject][triple.predicate].push(object);
                } else if (Array.isArray(acc[triple.subject][triple.predicate])){
                  acc[triple.subject][triple.predicate].push(object);
                } else {
                  acc[triple.subject][triple.predicate] = [object];
                }
              } else  {
                // deal with blanks
                if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
                acc[triple.subject][triple.predicate].push(object);
              }


              // ret[triple.subject] = ret[triple.subject] ? ret[triple.subject] : { '@id': triple.subject };
              // if (Array.isArray(ret[triple.subject][triple.predicate])) {
              //   ret[triple.subject][triple.predicate].push(triple.object);
              // } else {
              //   ret[triple.subject][triple.predicate] = [triple.object];
              // }
              return cb(null, acc);
            } else if (typeof checked === 'object') {
              // ret[triple.subject] = ret[triple.subject] ? ret[triple.subject] : { '@id': triple.subject };
              graphdb.get(checked, function(err, results) {
                // debug("err", err)
                if (err) return cb(err)
                // console.log("dynamic check : ", checked)
                // console.log("validated triple: " + JSON.stringify(triple,true,2))
                // console.log("results : ", results)
                if (results.length == 0) {

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
                  } else if (!N3Util.isBlank(triple.object)) {
                    var object = {};
                    // debug("triple", triple)
                    if (N3Util.isIRI(triple.object)) {
                      // debug("isIRI")
                      object['@id'] = triple.object;
                    } else if (N3Util.isLiteral(triple.object)) {
                      object = getCoercedObject(triple.object);
                    }
                    if(object['@id']) {
                      // expanding object iri
                      if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
                      acc[triple.subject][triple.predicate].push(object);
                    } else if (Array.isArray(acc[triple.subject][triple.predicate])){
                      acc[triple.subject][triple.predicate].push(object);
                    } else {
                      acc[triple.subject][triple.predicate] = [object];
                    }
                  } else  {
                    // deal with blanks
                    if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
                    acc[triple.subject][triple.predicate].push(object);
                  }

                  // if (Array.isArray(ret[triple.subject][triple.predicate])) {
                  //   ret[triple.subject][triple.predicate].push(triple.object);
                  // } else {
                  //   ret[triple.subject][triple.predicate] = [triple.object];
                  // }
                  // debug("ret", ret)
                } else {
                  // debug("conflict", triple)
                  conflicts.push(triple);
                }
                return cb(null, acc);
              });
            } else {
              // debug("fails doPut check: " + result)
              // debug("failed triple: " + JSON.stringify(triple,true,2))
              // debug("conflict", triple)
              conflicts.push(triple);
              return cb(null, acc);
            }
          }, function(err, result) {
            if (err) return cbGraph(err)
            // debug("result", result)
            graphs[graph_key] = result
            return cbGraph()
          });
        }, function(err) {
          // debug("err", err)
          if (err) callback(err,null)
          // TODO: Fix the problem with framing graphs. Named graph key values aren't returned at the root
          // as they are not processed correctly by the JSON-LD framing algorithm
          // debug("graphs", JSON.stringify(graphs,true,2))

          var checked = { "@graph": Object.keys(graphs).map(function(graph) {
            var keys = Object.keys(graphs[graph]);
            if (graph == "@default") {
              // map the default graph at the root.
              if (keys.length == 0) return null
              return Object.assign({
                "@id": graph,
              }, keys.map(function(key) { return graphs[graph][key] } ))
            } else {
              if (keys.length == 0) return null
              return Object.assign({
                "@id": graph,
                "@graph" : keys.map(function(key) { return graphs[graph][key] } )
              } )
            }
          }).filter(function(i) { return i }) };
          // debug("checked", JSON.stringify(checked, true,2))
          callback(null, conflicts, checked)
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
        // debug('before doPut')
        // debug(obj)
        // Default to Sync version for now as I can't make the streaming version perform.
        doPutSync(obj, options, callback);
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

    // console.log('obj', obj)

    jsonld.compact(obj, obj["@context"], function(err, compacted) {
      if (err) {
        return callback && callback(err);
      }
      // debug("compacted")
      // debug(JSON.stringify(compacted,true,2))

      doCheck(compacted, options, checkfn, function(err, conflicts, checked) {
        // debug("graph")
        // debug(JSON.stringify(graph,true,2))
        // debug("checked")
        // debug(JSON.stringify(checked,true,2))
        // debug("conflicts")
        // debug(JSON.stringify(conflicts,true,2))

        if (err || checked === null) {
          return callback(err, checked);
        }

        var frame = { "@context": obj["@context"] };

        // debug("frame")
        // debug(JSON.stringify(frame,true,2))

          jsonld.compact(checked, obj["@context"], options, function(err, compacted) {
            // debug("compacted")
            // debug(JSON.stringify(compacted,true,2))
            callback(null, { conflicts, checked: compacted } );
          });

        // jsonld.frame(checked, frame, function(err, framed) {
        //   if (err || framed === null) {
        //     return callback(err, framed);
        //   }
        //   // debug("framed")
        //   // debug(JSON.stringify(framed,true,2))
        //   var context = frame["@context"] || {};
        //
        //   jsonld.compact(framed, context, options, function(err, compacted) {
        //     // debug("compacted")
        //     // debug(JSON.stringify(compacted,true,2))
        //     callback(null, conflicts, compacted);
        //   });
        // });


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

  function fetchRDFList(head, acc, callback) {
    graphdb.get({ subject: head, predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#first' }, function(err, value) {
      // TODO: Deal with non literal list elements
      acc.push(getCoercedObject(value[0].object))
      graphdb.get({ subject: head, predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#rest' }, function(err, next) {
        if (next[0].object == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#nil') return callback(acc);
        fetchRDFList(next[0].object, acc, callback);
      })
    })

  }

  function fetchExpandedTriples(iri, frame, callback) {
    debug('iri', iri)
    debug('frame', frame)
    var memo = {};
    if (typeof frame === 'function') {
      callback = frame;
      frame = {};
    }
    function embed(frame) {
      return ( frame && frame["@embed"] !== "@never" || frame && frame["@embed"] === undefined )
            || frame === undefined
    }
    graphdb.get({ subject: iri }, function(err, triples) {
      if (err || triples.length === 0) {
        return callback(err, null);
      }
      async.reduce(triples, memo, function(acc, triple, cb) {
        debug('triple', JSON.stringify(triple, true,2))
        if (!acc[triple.subject] && !N3Util.isBlank(triple.subject)) {
          acc[triple.subject] = { '@id': triple.subject };
        } else if ( triple.predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#first' ) {
          return cb(null, acc);
        } else if ( triple.predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#rest' ) {
          return fetchRDFList(triple.subject, [], function(list) {
            acc[triple.subject] = acc[triple.subject] || { '@list' : [] }
            acc[triple.subject]['@list'] = list;
            return cb(null, acc);
          });
        } else if (N3Util.isBlank(triple.subject) && !acc[triple.subject]) {
          acc[triple.subject] = {};
        }
        if (triple.predicate === RDFTYPE) {
          if (acc[triple.subject]['@type']) {
            acc[triple.subject]['@type'].push(triple.object);
          } else {
            debug('triple.object', triple.object)
            debug('acc', JSON.stringify(acc,true,2))
            acc[triple.subject]['@type'] = [triple.object];
            debug('acc', JSON.stringify(acc,true,2))
          }
          return cb(null, acc);
        } else if ((triple.object !== null) && !N3Util.isBlank((typeof triple.object === "string") ? triple.object : "")) {
          var object = {};
          if (N3Util.isLiteral(triple.object)) {
            object = getCoercedObject(triple.object);
          } else if (N3Util.isIRI((typeof triple.object === "string") ? triple.object : "")) {
            object['@id'] = triple.object;
          } else {
            console.log("getCoercedObject.triple", triple)
            object = getCoercedObject(triple.object);
          }
          // console.log(object)
          if(object['@id'] && embed(frame) && embed(frame && frame[triple.predicate]) ) {
            // expanding object iri
            debug('going for it')
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
          } else if (object['@id'] && !embed(frame && frame[triple.predicate]) ){
            const otherFramingProperties = Object.keys(frame[triple.predicate]).filter(function(key) { return key != '@embed' })
            debug('otherFramingProperties', otherFramingProperties)

            if (otherFramingProperties.length != 0 ) {
              // there's a @embed @never but also other properties we need to match
              // so we need to get fetch the target IRI.
              fetchExpandedTriples(triple.object, frame && frame[triple.predicate], function(err, expanded) {
                if (!acc[triple.subject][triple.predicate]) acc[triple.subject][triple.predicate] = [];
                if (expanded !== null) {
                  acc[triple.subject][triple.predicate].push(expanded[triple.object]);
                } else {
                  acc[triple.subject][triple.predicate].push(object);
                }
                return cb(err, acc);
              });
            } else {
              debug('object', object)
              debug('acc', JSON.stringify(acc,true,2))
              acc[triple.subject][triple.predicate] = [object];
              debug('acc', JSON.stringify(acc,true,2))
              return cb(err, acc);
            }
          } else {
            acc[triple.subject][triple.predicate] = [object];
            return cb(err, acc);
          }
        } else if (N3Util.isBlank(triple.object) && embed(frame && frame[triple.predicate])) {
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
        } else {
          // triple.object is null
          console.log('triple.object', triple.object)
          console.log('N3Util.isLiteral(triple.object)', N3Util.isLiteral(triple.object))
          if (triple.object === null) return cb(err,acc)
          throw new Error('wat')
        }
      }, function(err, result) {
        return callback(err, result);
      });
    });
  }

  graphdb.jsonld.get = function(doc, frame, options, callback) {
    var iri;
    if (typeof options === 'function') {
      callback = options;
      options = {};
    } else if (typeof frame === 'function') {
      callback = frame;
      frame = (typeof doc === 'string') ? { } : JSON.parse(JSON.stringify(doc)) ;
      options = {};
    }

    var context = doc['@context'] || frame['@context'] || frame

    options.base = ( doc["@context"] && doc["@context"]["@base"] ) || ( frame && frame["@base"] ) || options.base || this.options.base;
    debug('options', options)
    debug('options.base', options.base ? "yes": "no")

    if (typeof doc === 'string') {
      iri = doc
      doc = {};
      doc["@id"] = _expandIri(context, iri, { base: options.base ? true : false });
    } else {
      iri = doc['@id']
      doc = doc['@type'] ? { '@context': doc['@context'], '@id': doc['@id'], '@type': doc['@type'] } : { '@context': doc['@context'], '@id': doc['@id'] }
      doc["@id"] = _expandIri(context, iri, { base: options.base ? true : false });
    }

    debug('doc', doc)
    debug('frame', frame)

    // compacting the frame with an empty context will expand IRIs
    jsonld.compact(frame, {}, function(err, expanded_frame) {
      if (err) {
        return callback(err, null);
      }

      debug('expanded_frame', expanded_frame)
      jsonld.frame(doc, {}, function(err, framed) {
        // Framing the requested object with an empty frame
        // makes both graphs and resources accessible in a flat array.
        if (err || framed === null) {
          return callback(err, null);
        }
        var result = [];
        debug('framed', framed)
        const graph = ( framed['@graph'].length != 0 ) ? framed['@graph'] : [ doc ] ;
        debug('graph', graph)
        async.eachSeries(graph, function(item, cb) {
          iri = N3Util.isIRI(item["@id"]) ? item["@id"] : options.base + item["@id"]

          debug('iri', iri)
          debug('item', item)

          var key = N3Util.isBlank(item['@id']) ? '@default' : item['@id'];
          // result[key] = {};
          fetchExpandedTriples(iri, expanded_frame, function(err, expanded) {
            debug("expanded", JSON.stringify(expanded,true,2))
            if (expanded) result.push(expanded[key]);
            cb();
          });
        }, function(err) {
          debug("result", JSON.stringify(result,true,2))
          if (err || result === null) {
            debug(err)
            return callback(err, result);
          }

          if (result.length==0) callback(null, null)
          debug('frame', frame)
          var resultGraph = { "@context": context, "@graph": result }
          jsonld.frame(resultGraph, frame, function(err, framed) {
            debug('framed', JSON.stringify(framed,true,2))

            if (err || framed === null) {
              return callback(err, framed);
            }
            debug('context', JSON.stringify(context,true,2))
            // debug('options', options)
            var unblanked = unblank(framed)
            unblanked['@context'] = context
            debug('unblanked', JSON.stringify(unblanked,true,2))
            jsonld.compact(unblanked, context, options, function(err, result) {
              if (err) {
                debug(err)
                return callback(err, result);
              }
              debug('result', JSON.stringify(result,true,2))
              callback(err, result)
            });
          });
        })
      });
    })
  };

  return graphdb;
}

module.exports = levelgraphJSONLD;

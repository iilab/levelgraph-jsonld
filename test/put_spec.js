var expect = require('chai').expect;
var helper = require('./helper');
var async = require('async');
// var atomic = require('level-atomic');
var deasync = require('deasync');
// require('longjohn');

describe('jsonld.put', function() {
  var db, manu;

  beforeEach(function() {
    db = helper.getDB();
    manu = helper.getFixture('manu.json');
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should accept a done callback', function(done) {
    db.jsonld.put(manu, done);
  });

  it('should store a triple', function(done) {
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should store two triples', function(done) {
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person'
        },
        function(err, triples) {
          expect(triples).to.have.length(2);
          done();
        }
      );
    });
  });

  it('should store a JSON file', function(done) {
    db.jsonld.put(JSON.stringify(manu), function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person'
        },
        function(err, triples) {
          expect(triples).to.have.length(2);
          done();
        }
      );
    });
  });

  it('should support a base IRI', function(done) {
    manu['@id'] = '42';
    db.jsonld.put(manu, { base: 'http://levelgraph.org/tests/' }, function() {
      db.get(
        {
          subject: 'http://levelgraph.org/tests/42',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should generate an @id for unknown objects', function(done) {
    delete manu['@id'];
    var baseString = 'http://levelgraph.org/tests/';
    var baseRegEx = /^_\:/;

    db.jsonld.put(manu, { base: baseString }, function(err) {
      db.search(
        {
          subject: db.v('subject'),
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, solutions) {
          expect(solutions[0].subject).to.match(baseRegEx);
          done();
        }
      );
    });
  });

  it('should generate an @id for all blank nodes in a complex object', function(done) {
    var tesla = helper.getFixture('tesla.json');

    db.jsonld.put(tesla, { blank_ids: true }, function(err, obj) {
      expect(obj['gr:hasPriceSpecification']['@id']).to.match(/^_\:/);
      expect(obj['gr:includes']['@id']).to.match(/^_\:/);
      done();
    });
  });

  it('should pass the generated @id to callback', function(done) {
    delete manu['@id'];
    var baseString = 'http://levelgraph.org/tests/';
    var baseRegEx = /^_\:/;

    db.jsonld.put(manu, { base: baseString, blank_ids: true }, function(err, obj) {
      expect(obj['@id']).to.match(baseRegEx);
      done();
    });
  });

  it('should convert @type into http://www.w3.org/1999/02/22-rdf-syntax-ns#type', function(done) {
    db.jsonld.put(helper.getFixture('tesla.json'), function() {
      db.get(
        {
          subject: 'http://example.org/cars/for-sale#tesla',
          predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
          object: 'http://purl.org/goodrelations/v1#Offering'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should update a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      instance.homepage = 'http://another/website';
      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage',
            object: 'http://another/website'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should add a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      instance.age = 42;
      instance['@context'].age = 'http://xmlns.com/foaf/0.1/age';

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/age',
            object: '"42"^^http://www.w3.org/2001/XMLSchema#integer'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should delete a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      delete instance.homepage;

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should overwrite properties with the overwrite option', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      delete instance.homepage;

      db.jsonld.put(instance, { overwrite: true }, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should delete a nested object', function(done) {
    db.jsonld.put(helper.getFixture('tesla.json'), function(err, instance) {
      delete instance['gr:hasPriceSpecification'];

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://example.org/cars/for-sale#tesla',
            predicate: 'http://purl.org/goodrelations/v1#'
          },
          function(err, triples) {
            expect(triples).to.be.empty;
            done();
          }
        );
      });
    });
  });

  it('should receive error on invalid input', function(done) {
    var invalid = {
      '@context': { '@vocab': 'http//example.com/' },
      test: { '@value': 'foo', bar: 'oh yes' }
    };
    db.jsonld.put(invalid, function(err) {
      expect(err && err.name).to.equal('jsonld.SyntaxError');
      expect(err && err.message).to.equal('Invalid JSON-LD syntax; the value of "@vocab" in a @context must be an absolute IRI.');
      done();
    });
  });

  it('should manage mapped @id', function(done) {
    var mapped_id = helper.getFixture('mapped_id.json');
    db.jsonld.put(mapped_id, { preserve: true }, function(err, obj) {
      expect(err && err.name).to.be.null;
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(4);
        done();
      });
    });
  });

  it('should insert graphs', function(done) {
    var library = helper.getFixture('library.json');

    db.jsonld.put(library, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(9);
        done();
      });
    });
  });

  it('should insert a simple graph', function(done) {
    var a = {
      '@context': { '@vocab': 'http://example.org/vocab/', '@base': 'http://example.org/' },
      '@graph': [
        {
          '@id': 'a',
          is: 'stuff'
        }
      ]
    };

    db.jsonld.put(a, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(1);
        done();
      });
    });
  });

  it('should insert a simple named graph', function(done) {
    var a = {
      '@context': { '@vocab': 'http://example.org/vocab/', '@base': 'http://example.org/' },
      '@id': 'g',
      '@graph': [
        {
          '@id': 'a',
          is: 'stuff'
        }
      ]
    };

    db.jsonld.put(a, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(1);
        done();
      });
    });
  });

  it('should insert a complex named graph', function(done) {
    var named_graph = helper.getFixture('named_graph.json');

    db.jsonld.put(named_graph, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(7);
        done();
      });
    });
  });

  it('should insert a document with multiple named graph', function(done) {
    var three_graphs = helper.getFixture('three_graphs.json');

    db.jsonld.put(three_graphs, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(3);
        done();
      });
    });
  });

  it('should insert a document with a top named graph and sub named graphs', function(done) {
    var top_sub_graphs = helper.getFixture('top_sub_graphs.json');

    db.jsonld.put(top_sub_graphs, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(4);
        done();
      });
    });
  });

  it('should insert a document with a sub graph referring the top graphs', function(done) {
    var graph_as_node = helper.getFixture('graph_as_node.json');

    db.jsonld.put(graph_as_node, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(3);
        done();
      });
    });
  });

  it('should insert a large graph', function(done) {
    var ontology = helper.getFixture('ontology.json');

    db.jsonld.put(ontology, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(334);
        done();
      });
    });
  });

  it('should deal with overwriting', function(done) {
    var a = { '@context': { '@vocab': 'http://example.org/vocab/', '@base': 'http://example.org/' }, '@id': 'a', is: 'stuff' };
    var b = { '@context': { '@vocab': 'http://example.org/vocab/', '@base': 'http://example.org/' }, '@id': 'b', is: 'morestuff' };
    var a2 = { '@context': { '@vocab': 'http://example.org/vocab/', '@base': 'http://example.org/' }, '@id': 'a', is: 'morestuff' };

    var ag = {
      subject: 'http://example.org/a',
      predicate: 'http://example.org/vocab/is',
      object: '"morestuff"'
      // graph:""
    };

    var ag2 = {
      subject: 'http://example.org/a',
      predicate: 'http://example.org/vocab/is',
      object: '"morestuff"'
      // graph:""
    };

    db.jsonld.put(a, function() {
      db.jsonld.put(b, function() {
        db.get({}, function(err, triples) {
          console.log('put.triples', triples);
        });
        db.jsonld.del(a2, function() {
          db.jsonld.put(a2, function() {
            db.put([ag, ag2], function(err) {
              if (err) console.log(err);
              db.get({}, function(err, triples) {
                console.log('triples', triples);
                expect(triples).to.have.length(3);
                done();
              });
            });
          });
        });
      });
    });
  });

  it('should handle @list', function(done) {
    var list = helper.getFixture('list.json');

    db.jsonld.put(list, function() {
      db.jsonld.get(list, function(err, obj) {
        expect(obj).to.deep.equal(list);
        done();
      });
    });
  });

  it('should handle multiple graphs', function(done) {
    var graphs = helper.getFixture('multi_graphs.json');

    db.jsonld.put(graphs, function() {
      db.jsonld.get(graphs, function(err, obj) {
        console.log(obj);
        db.get({}, function(err, triples) {
          console.log(triples);
          expect(triples).to.have.length(3);
          done();
        });
      });
    });
  });
});

describe('jsonld.put with default base and overwrite and cut option set to true (backward compatibility)', function() {
  var db, manu;

  beforeEach(function() {
    db = helper.getDB({ jsonld: { base: 'http://levelgraph.io/ahah/', overwrite: true, cut: true } });
    manu = helper.getFixture('manu.json');
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should use it', function(done) {
    manu['@id'] = '42';
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://levelgraph.io/ahah/42',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should correctly generate blank nodes as subjects', function(done) {
    var tesla = helper.getFixture('tesla.json');

    db.jsonld.put(tesla, function() {
      db.search(
        [
          {
            subject: 'http://example.org/cars/for-sale#tesla',
            predicate: 'http://purl.org/goodrelations/v1#hasPriceSpecification',
            object: db.v('bnode')
          },
          {
            subject: db.v('bnode'),
            predicate: 'http://purl.org/goodrelations/v1#hasCurrency',
            object: '"USD"'
          }
        ],
        function(err, solutions) {
          expect(solutions[0].bnode).to.exist;
          done();
        }
      );
    });
  });

  it('should not store undefined objects', function(done) {
    var tesla = helper.getFixture('tesla.json');

    db.jsonld.put(tesla, function() {
      db.get({}, function(err, triples) {
        triples.forEach(function(triple) {
          expect(triple.object).to.exist;
        });
        done();
      });
    });
  });

  it('should support nested objects', function(done) {
    var nested = helper.getFixture('nested.json');

    db.jsonld.put(nested, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(5);
        done();
      });
    });
  });

  it('should break compatibility and not delete existing triples', function(done) {
    var chapter = helper.getFixture('chapter.json');
    var description = helper.getFixture('chapterdescription.json');

    db.jsonld.put(chapter, function() {
      db.jsonld.put(description, function() {
        db.get({}, function(err, triples) {
          expect(triples).to.have.length(3);
          done();
        });
      });
    });
  });

  it('should not delete existing facts with the cut option set to false', function(done) {
    var chapter = helper.getFixture('chapter.json');
    var description = helper.getFixture('chapterdescription.json');

    db.jsonld.put(chapter, function() {
      db.jsonld.put(description, { cut: false }, function() {
        db.get({}, function(err, triples) {
          expect(triples).to.have.length(3);
          done();
        });
      });
    });
  });

  it('should insert graphs', function(done) {
    var library = helper.getFixture('library.json');

    db.jsonld.put(library, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(9);
        done();
      });
    });
  });
});

describe('jsonld.put with base', function() {
  var db, manu;

  beforeEach(function() {
    db = helper.getDB({ jsonld: { base: 'http://levelgraph.io/ahah/' } });
    manu = helper.getFixture('manu.json');
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should accept a done callback', function(done) {
    db.jsonld.put(manu, done);
  });

  it('should store a triple', function(done) {
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should store two triples', function(done) {
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person'
        },
        function(err, triples) {
          expect(triples).to.have.length(2);
          done();
        }
      );
    });
  });

  it('should store a JSON file', function(done) {
    db.jsonld.put(JSON.stringify(manu), function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person'
        },
        function(err, triples) {
          expect(triples).to.have.length(2);
          done();
        }
      );
    });
  });

  it('should support a base IRI', function(done) {
    manu['@id'] = '42';
    db.jsonld.put(manu, { base: 'http://levelgraph.org/tests/' }, function() {
      db.get(
        {
          subject: 'http://levelgraph.org/tests/42',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should generate an @id for unknown objects with the blank_ids option', function(done) {
    delete manu['@id'];
    var baseString = 'http://levelgraph.org/tests/';
    var baseRegEx = /^_\:/;

    db.jsonld.put(manu, { base: baseString, blank_ids: true }, function() {
      db.search(
        {
          subject: db.v('subject'),
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, solutions) {
          expect(solutions[0].subject).to.match(baseRegEx);
          done();
        }
      );
    });
  });

  it('should pass the generated @id to callback with the blank_ids option', function(done) {
    delete manu['@id'];
    var baseString = 'http://levelgraph.org/tests/';
    var baseRegEx = /^_\:/;

    db.jsonld.put(manu, { base: baseString, blank_ids: true }, function(err, obj) {
      expect(obj['@id']).to.match(baseRegEx);
      done();
    });
  });

  it('should convert @type into http://www.w3.org/1999/02/22-rdf-syntax-ns#type', function(done) {
    db.jsonld.put(helper.getFixture('tesla.json'), function() {
      db.get(
        {
          subject: 'http://example.org/cars/for-sale#tesla',
          predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
          object: 'http://purl.org/goodrelations/v1#Offering'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should update a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      instance.homepage = 'http://another/website';
      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage',
            object: 'http://another/website'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should add a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      instance.age = 42;
      instance['@context'].age = 'http://xmlns.com/foaf/0.1/age';

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/age',
            object: '"42"^^http://www.w3.org/2001/XMLSchema#integer'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should delete a property but preserve existing ones', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      delete instance.homepage;

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should preserve properties with the preserve option', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      delete instance.homepage;

      db.jsonld.put(instance, { preserve: true }, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should use the base option', function(done) {
    manu['@id'] = '42';
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://levelgraph.io/ahah/42',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should correctly generate blank nodes as subjects', function(done) {
    var tesla = helper.getFixture('tesla.json');

    db.jsonld.put(tesla, function() {
      db.search(
        [
          {
            subject: 'http://example.org/cars/for-sale#tesla',
            predicate: 'http://purl.org/goodrelations/v1#hasPriceSpecification',
            object: db.v('bnode')
          },
          {
            subject: db.v('bnode'),
            predicate: 'http://purl.org/goodrelations/v1#hasCurrency',
            object: '"USD"'
          }
        ],
        function(err, solutions) {
          expect(solutions[0].bnode).to.exist;
          done();
        }
      );
    });
  });

  it('should not store undefined objects', function(done) {
    var tesla = helper.getFixture('tesla.json');

    db.jsonld.put(tesla, function() {
      db.get({}, function(err, triples) {
        triples.forEach(function(triple) {
          expect(triple.object).to.exist;
        });
        done();
      });
    });
  });

  it('should delete a nested object', function(done) {
    db.jsonld.put(helper.getFixture('tesla.json'), function(err, instance) {
      delete instance['gr:hasPriceSpecification'];

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://example.org/cars/for-sale#tesla',
            predicate: 'http://purl.org/goodrelations/v1#'
          },
          function(err, triples) {
            expect(triples).to.be.empty;
            done();
          }
        );
      });
    });
  });

  it('should receive error on invalid input', function(done) {
    var invalid = {
      '@context': { '@vocab': 'http//example.com/' },
      test: { '@value': 'foo', bar: 'oh yes' }
    };
    db.jsonld.put(invalid, function(err) {
      expect(err && err.name).to.equal('jsonld.SyntaxError');
      expect(err && err.message).to.equal('Invalid JSON-LD syntax; the value of "@vocab" in a @context must be an absolute IRI.');
      done();
    });
  });

  it('should not overwrite existing facts', function(done) {
    var chapter = helper.getFixture('chapter.json');
    var description = helper.getFixture('chapterdescription.json');

    db.jsonld.put(chapter, function() {
      db.jsonld.put(description, function() {
        db.get({}, function(err, triples) {
          expect(triples).to.have.length(3);
          done();
        });
      });
    });
  });

  it('should insert graphs', function(done) {
    var library = helper.getFixture('library.json');

    db.jsonld.put(library, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(9);
        done();
      });
    });
  });
});

describe('jsonld.put with overwrite and cut set to true (backward compatibility)', function() {
  var db, manu;

  beforeEach(function() {
    db = helper.getDB({ jsonld: { overwrite: false, cut: true } });
    manu = helper.getFixture('manu.json');
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should accept a done callback', function(done) {
    db.jsonld.put(manu, done);
  });

  it('should store a triple', function(done) {
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should store two triples', function(done) {
    db.jsonld.put(manu, function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person'
        },
        function(err, triples) {
          expect(triples).to.have.length(2);
          done();
        }
      );
    });
  });

  it('should store a JSON file', function(done) {
    db.jsonld.put(JSON.stringify(manu), function() {
      db.get(
        {
          subject: 'http://manu.sporny.org#person'
        },
        function(err, triples) {
          expect(triples).to.have.length(2);
          done();
        }
      );
    });
  });

  it('should support a base IRI', function(done) {
    manu['@id'] = '42';
    db.jsonld.put(manu, { base: 'http://levelgraph.org/tests/' }, function() {
      db.get(
        {
          subject: 'http://levelgraph.org/tests/42',
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should generate an @id for unknown objects', function(done) {
    delete manu['@id'];
    var baseString = 'http://levelgraph.org/tests/';
    var baseRegEx = /^_\:/;

    db.jsonld.put(manu, { base: baseString, blank_ids: true }, function() {
      db.search(
        {
          subject: db.v('subject'),
          predicate: 'http://xmlns.com/foaf/0.1/name'
        },
        function(err, solutions) {
          expect(solutions[0].subject).to.match(baseRegEx);
          done();
        }
      );
    });
  });

  it('should generate an @id for all blank nodes in a complex object', function(done) {
    var tesla = helper.getFixture('tesla.json');

    db.jsonld.put(tesla, { blank_ids: true }, function(err, obj) {
      expect(obj['gr:hasPriceSpecification']['@id']).to.match(/^_\:/);
      expect(obj['gr:includes']['@id']).to.match(/^_\:/);
      done();
    });
  });

  it('should pass the generated @id to callback', function(done) {
    delete manu['@id'];
    var baseString = 'http://levelgraph.org/tests/';
    var baseRegEx = /^_\:/;

    db.jsonld.put(manu, { base: baseString, blank_ids: true }, function(err, obj) {
      expect(obj['@id']).to.match(baseRegEx);
      done();
    });
  });

  it('should convert @type into http://www.w3.org/1999/02/22-rdf-syntax-ns#type', function(done) {
    db.jsonld.put(helper.getFixture('tesla.json'), function() {
      db.get(
        {
          subject: 'http://example.org/cars/for-sale#tesla',
          predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
          object: 'http://purl.org/goodrelations/v1#Offering'
        },
        function(err, triples) {
          expect(triples).to.have.length(1);
          done();
        }
      );
    });
  });

  it('should update a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      instance.homepage = 'http://another/website';
      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage',
            object: 'http://another/website'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should add a property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      instance.age = 42;
      instance['@context'].age = 'http://xmlns.com/foaf/0.1/age';

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/age',
            object: '"42"^^http://www.w3.org/2001/XMLSchema#integer'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should break compatibility and keep an existing property', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      delete instance.homepage;

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should preserve properties with the overwrite option set to false', function(done) {
    db.jsonld.put(manu, function(err, instance) {
      delete instance.homepage;

      db.jsonld.put(instance, { overwrite: false }, function() {
        db.get(
          {
            subject: 'http://manu.sporny.org#person',
            predicate: 'http://xmlns.com/foaf/0.1/homepage'
          },
          function(err, triples) {
            expect(triples).to.have.length(1);
            done();
          }
        );
      });
    });
  });

  it('should delete a nested object', function(done) {
    db.jsonld.put(helper.getFixture('tesla.json'), function(err, instance) {
      delete instance['gr:hasPriceSpecification'];

      db.jsonld.put(instance, function() {
        db.get(
          {
            subject: 'http://example.org/cars/for-sale#tesla',
            predicate: 'http://purl.org/goodrelations/v1#'
          },
          function(err, triples) {
            expect(triples).to.be.empty;
            done();
          }
        );
      });
    });
  });

  it('should receive error on invalid input', function(done) {
    var invalid = {
      '@context': { '@vocab': 'http//example.com/' },
      test: { '@value': 'foo', bar: 'oh yes' }
    };
    db.jsonld.put(invalid, function(err) {
      expect(err && err.name).to.equal('jsonld.SyntaxError');
      expect(err && err.message).to.equal('Invalid JSON-LD syntax; the value of "@vocab" in a @context must be an absolute IRI.');
      done();
    });
  });

  it('should manage mapped @id', function(done) {
    var mapped_id = helper.getFixture('mapped_id.json');
    db.jsonld.put(mapped_id, { preserve: true }, function(err) {
      expect(err && err.name).to.be.null;
      db.jsonld.get(mapped_id['id'], { '@context': mapped_id['@context'] }, function(err, obj) {
        db.get({}, function(err, triples) {
          expect(triples).to.have.length(4);
          done();
        });
      });
    });
  });

  it('should insert graphs', function(done) {
    var library = helper.getFixture('library.json');

    db.jsonld.put(library, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(9);
        done();
      });
    });
  });

  it('should insert documents with null keys', function(done) {
    var null_keys = helper.getFixture('null_keys.json');

    db.jsonld.put(null_keys, function() {
      db.get({}, function(err, triples) {
        expect(triples).to.have.length(2);
        done();
      });
    });
  });

  it('should put rapidly', function(done) {
    console.time('createdeep');
    // 10000 doesn't pass. 5000 used to pass
    var length = 1000;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    console.timeEnd('createdeep');
    console.time('put');

    db.jsonld.put(
      {
        '@context': {
          link: {
            '@id': 'http://example.org/link#',
            '@type': '@id'
          },
          '@base': 'https://levelgraph.io/get/',
          '@vocab': 'http://example.org/vocab#'
        },
        '@graph': deep
      },
      function(err, obj) {
        console.timeEnd('put');
        // console.log(JSON.stringify(obj,true,2));
        expect(obj['@graph'][length - 1]['link']).to.eql(length.toString());
        expect(obj['@graph'][length - 1]['value']).to.eql((length - 1).toString());
        done();
      }
    );
  });

  it('should put atomically simply', function(done) {
    var length = 10; // used to work with 100
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    console.time('put atomically');

    async.eachSeries(
      deep,
      function(triple, cb) {
        console.time('put' + triple['@id']);
        db.jsonld.put(
          {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            },
            '@graph': triple
          },
          function(err, obj) {
            if (err) console.log('spec error', err);
            console.timeEnd('put' + triple['@id']);
            // console.log(triple)
            console.time('get' + triple['@id']);
            db.jsonld.get('https://levelgraph.io/get/' + triple['@id'], function(err, obj) {
              console.timeEnd('get' + triple['@id']);
              if (err) console.log(err);
              // console.log(JSON.stringify(obj,true,2));
              expect(obj['http://example.org/vocab#value']).to.eql(triple['value']);
              cb();
            });
          }
        );
      },
      function() {
        console.timeEnd('put atomically');
        done();
      }
    );
  });

  it('should check and put atomically when ran in series', function(done) {
    var length = 5;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    const validate = function({ subject, object, predicate, graph }) {
      // Ignore @type
      if (predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type') return true;
      // Property unicity: Conflict if subject predicate is the same as an existing triple.
      return { subject, predicate };
    };
    console.time('check atomically');

    var previous_triple = {};

    async.eachSeries(
      deep,
      function(triple, cb) {
        db.jsonld.check(
          {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            },
            '@graph': previous_triple
          },
          validate,
          function(err, conflicts, checked) {
            db.jsonld.put(
              {
                '@context': {
                  link: {
                    '@id': 'http://example.org/link#',
                    '@type': '@id'
                  },
                  '@base': 'https://levelgraph.io/get/',
                  '@vocab': 'http://example.org/vocab#'
                },
                '@graph': triple
              },
              { sync: true },
              function(err, obj) {
                if (err) console.log('Err', err);
                db.jsonld.get('https://levelgraph.io/get/' + triple['@id'], function(err, obj) {
                  if (err) console.log(err);
                  db.get({}, function(err, triples) {
                    if (Object.keys(previous_triple).length !== 0) {
                      expect(conflicts).to.have.length(2);
                      expect(obj['http://example.org/vocab#value']).to.eql(triple['value']);
                    }
                    expect(triples).to.have.length((parseInt(triple['value']) + 1) * 2);
                    previous_triple = triple;
                    cb();
                  });
                });
              }
            );
          }
        );
      },
      function() {
        console.timeEnd('check atomically');
        done();
      }
    );
  });

  it('does not check and put atomically when ran in parallel', function(done) {
    var length = 50;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    const validate = function({ subject, object, predicate, graph }) {
      // Ignore @type
      if (predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type') return true;
      // Property unicity: Conflict if subject predicate is the same as an existing triple.
      return { subject, predicate };
    };

    var previous_triple = {};

    async.each(
      deep,
      function(triple, cb) {
        db.jsonld.check(
          {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            },
            '@graph': previous_triple
          },
          validate,
          function(err, conflicts, checked) {
            db.jsonld.put(
              {
                '@context': {
                  link: {
                    '@id': 'http://example.org/link#',
                    '@type': '@id'
                  },
                  '@base': 'https://levelgraph.io/get/',
                  '@vocab': 'http://example.org/vocab#'
                },
                '@graph': triple
              },
              function(err, obj) {
                if (err) console.log('Err', err);
                db.jsonld.get('https://levelgraph.io/get/' + triple['@id'], function(err, obj) {
                  if (err) console.log(err);
                  if (Object.keys(previous_triple).length !== 0) {
                    expect(conflicts).to.have.length(0);
                    expect(obj['http://example.org/vocab#value']).to.eql(triple['value']);
                  }
                  previous_triple = triple;
                  cb();
                });
              }
            );
          }
        );
      },
      function() {
        done();
      }
    );
  });

  it('should check and put atomically when ran with deasync in parallel', function(done) {
    var length = 50;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    const validate = function({ subject, object, predicate, graph }) {
      // Ignore @type
      if (predicate == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type') return true;
      // Property unicity: Conflict if subject predicate is the same as an existing triple.
      return { subject, predicate };
    };

    var previous_triple = {};

    async.each(
      deep,
      function(triple, cb) {
        var done = false;
        db.jsonld.check(
          {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            },
            '@graph': previous_triple
          },
          validate,
          function(err, conflicts, checked) {
            db.jsonld.put(
              {
                '@context': {
                  link: {
                    '@id': 'http://example.org/link#',
                    '@type': '@id'
                  },
                  '@base': 'https://levelgraph.io/get/',
                  '@vocab': 'http://example.org/vocab#'
                },
                '@graph': triple
              },
              function(err, obj) {
                if (err) console.log('Err', err);
                db.jsonld.get('https://levelgraph.io/get/' + triple['@id'], function(err, obj) {
                  if (err) console.log(err);
                  db.get({}, function(err, triples) {
                    if (Object.keys(previous_triple).length !== 0) {
                      expect(conflicts).to.have.length(2);
                      expect(obj['http://example.org/vocab#value']).to.eql(triple['value']);
                    }
                    expect(triples).to.have.length((parseInt(triple['value']) + 1) * 2);
                    previous_triple = triple;
                    done = true;
                  });
                });
              }
            );
          }
        );
        deasync.loopWhile(function() {
          return !done;
        });
        cb();
      },
      function() {
        done();
      }
    );
  });

  it('does not put atomically when ran in series', function(done) {
    // i.e All the put complete before any get is done. So the first get will see all the puts.

    var length = 5;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `a${k + 1}`
      };
    });

    console.time('check atomically');

    async.eachSeries(
      deep,
      function(triple, cb) {
        console.time('put' + triple['@id']);
        db.jsonld.put(
          Object.assign(triple, {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            }
          }),
          /*{sync:true},*/ function(err, obj) {
            if (err) console.log('Err', err);
            console.timeEnd('put' + triple['@id']);
            // console.log(triple)
            console.time('get' + triple['@id']);
            // setTimeout(function() {
            console.log('get()', 'https://levelgraph.io/get/' + triple['@id']);
            db.jsonld.get(
              'https://levelgraph.io/get/' + triple['@id'],
              {
                link: {
                  '@id': 'http://example.org/link#',
                  '@type': '@id'
                },
                '@base': 'https://levelgraph.io/get/',
                '@vocab': 'http://example.org/vocab#'
              },
              { base: 'https://levelgraph.io/get/' },
              function(err, obj) {
                console.timeEnd('get' + triple['@id']);
                if (err) console.log(err);
                db.get({}, function(err, triples) {
                  expect(triples).to.have.length((parseInt(triple['value']) + 1) * 2);
                  cb();
                });
                // console.log("obj", JSON.stringify(obj,true,2));
                // expect(obj["http://example.org/vocab#value"]).to.eql(triple["value"] );
                // cb();
              }
            );
            // }, 10)
          }
        );
      },
      function(err) {
        if (err) console.log(err);
        console.timeEnd('check atomically');
        done();
      }
    );
  });

  it('does not put atomically when ran in parallel', function(done) {
    // i.e All the put complete before any get is done. So the first get will see all the puts.

    var length = 5;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `a${k + 1}`
      };
    });

    console.time('check atomically');

    async.each(
      deep,
      function(triple, cb) {
        console.time('put' + triple['@id']);
        db.jsonld.put(
          Object.assign(triple, {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            }
          }),
          /*{sync:true},*/ function(err, obj) {
            if (err) console.log('Err', err);
            console.timeEnd('put' + triple['@id']);
            // console.log(triple)
            console.time('get' + triple['@id']);
            // setTimeout(function() {
            console.log('get()', 'https://levelgraph.io/get/' + triple['@id']);
            db.jsonld.get(
              'https://levelgraph.io/get/' + triple['@id'],
              {
                link: {
                  '@id': 'http://example.org/link#',
                  '@type': '@id'
                },
                '@base': 'https://levelgraph.io/get/',
                '@vocab': 'http://example.org/vocab#'
              },
              { base: 'https://levelgraph.io/get/' },
              function(err, obj) {
                console.timeEnd('get' + triple['@id']);
                if (err) console.log(err);
                db.get({}, function(err, triples) {
                  expect(triples).to.have.length(10);
                  cb();
                });
                // console.log("obj", JSON.stringify(obj,true,2));
                // expect(obj["http://example.org/vocab#value"]).to.eql(triple["value"] );
                // cb();
              }
            );
            // }, 10)
          }
        );
      },
      function(err) {
        if (err) console.log(err);
        console.timeEnd('check atomically');
        done();
      }
    );
  });

  it('should put atomically when ran with deasync in parallel', function(done) {
    var length = 5;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    console.time('check atomically');

    async.each(
      deep,
      function(triple, cb) {
        console.time('put' + triple['@id']);
        var done = false;
        db.jsonld.put(
          Object.assign(triple, {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            }
          }),
          /*{sync:true},*/ function(err, obj) {
            if (err) console.log('Err', err);
            console.timeEnd('put' + triple['@id']);
            // console.log(triple)
            console.time('get' + triple['@id']);
            setImmediate(function() {
              console.log('get()', 'https://levelgraph.io/get/' + triple['@id']);
              db.jsonld.get(
                'https://levelgraph.io/get/' + triple['@id'],
                {
                  link: {
                    '@id': 'http://example.org/link#',
                    '@type': '@id'
                  },
                  '@base': 'https://levelgraph.io/get/',
                  '@vocab': 'http://example.org/vocab#'
                },
                { base: 'https://levelgraph.io/get/' },
                function(err, obj) {
                  console.timeEnd('get' + triple['@id']);
                  if (err) console.log(err);
                  db.get({}, function(err, triples) {
                    expect(triples).to.have.length((parseInt(triple['value']) + 1) * 2);
                    console.log(triples.length);
                    done = true;
                  });
                  // console.log("obj", JSON.stringify(obj,true,2));
                  // expect(obj["http://example.org/vocab#value"]).to.eql(triple["value"] );
                  // cb();
                }
              );
            });
          }
        );
        deasync.loopWhile(function() {
          return !done;
        });
        cb();
      },
      function(err) {
        if (err) console.log(err);
        console.timeEnd('check atomically');
        done();
      }
    );
  });

  it('should put atomically when ran with a lock in parallel', function(done) {
    var length = 5;
    var deep = Array.from({ length: length }, function(v, k) {
      return {
        '@id': `${k}`,
        value: `${k}`,
        link: `${k + 1}`
      };
    });

    console.time('check atomically');

    async.each(
      deep,
      function(triple, cb) {
        console.time('put' + triple['@id']);
        db.jsonld.put(
          Object.assign(triple, {
            '@context': {
              link: {
                '@id': 'http://example.org/link#',
                '@type': '@id'
              },
              '@base': 'https://levelgraph.io/get/',
              '@vocab': 'http://example.org/vocab#'
            }
          }),
          /*{sync:true},*/ function(err, obj) {
            if (err) console.log('Err', err);
            console.timeEnd('put' + triple['@id']);
            // console.log(triple)
            console.time('get' + triple['@id']);
            setImmediate(function() {
              console.log('get()', 'https://levelgraph.io/get/' + triple['@id']);
              db.jsonld.get(
                'https://levelgraph.io/get/' + triple['@id'],
                {
                  link: {
                    '@id': 'http://example.org/link#',
                    '@type': '@id'
                  },
                  '@base': 'https://levelgraph.io/get/',
                  '@vocab': 'http://example.org/vocab#'
                },
                { base: 'https://levelgraph.io/get/' },
                function(err, obj) {
                  console.timeEnd('get' + triple['@id']);
                  if (err) console.log(err);
                  db.get({}, function(err, triples) {
                    expect(triples).to.have.length((parseInt(triple['value']) + 1) * 2);
                    console.log(triples.length);
                    cb();
                  });
                  // console.log("obj", JSON.stringify(obj,true,2));
                  // expect(obj["http://example.org/vocab#value"]).to.eql(triple["value"] );
                  // cb();
                }
              );
            });
          }
        );
      },
      function(err) {
        if (err) console.log(err);
        console.timeEnd('check atomically');
        done();
      }
    );
  });
});

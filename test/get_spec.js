var expect = require('chai').expect;
var helper = require('./helper');

describe('jsonld.get', function() {

  var db, manu;

  beforeEach(function() {
    manu = helper.getFixture('manu.json');
    db = helper.getDB({ jsonld: { base: 'http://levelgraph.io/get' } });
  });

  afterEach(function(done) {
    db.close(done);
  });

  it('should get no object', function(done) {
    db.jsonld.get('http://path/to/nowhere', { '@context': manu['@context'] }, function(err, obj) {
      expect(obj).to.be.null;
      done();
    });
  });

  describe('with one object loaded', function() {
    beforeEach(function(done) {
      db.jsonld.put(manu, done);
    });

    it('should load it', function(done) {
      db.jsonld.get(manu['@id'], { '@context': manu['@context'] }, function(err, obj) {
        expect(obj).to.eql(manu);
        done();
      });
    });
  });

  describe('with an object with blank nodes', function() {
    var tesla, annotation;

    beforeEach(function(done) {
      tesla = helper.getFixture('tesla.json');
      annotation = helper.getFixture('annotation.json');
      done();
    });

    it('should load it properly', function(done) {
      db.jsonld.put(tesla, function() {
        db.jsonld.get(tesla['@id'], { '@context': tesla['@context'] }, function(err, obj) {
          expect(obj).to.eql(tesla);
          done();
        });
      });
    });

    it('should load a context with mapped ids', function(done) {
      db.jsonld.put(annotation, function() {
        db.jsonld.get(annotation['id'], { '@context': annotation['@context'] }, function(err, obj) {
          expect(obj['body']).to.deep.have.members(annotation['body']);
          expect(obj['target']).to.deep.have.members(annotation['target']);
          done();
        });
      });
    });
  });

  it('should support nested objects', function(done) {
    var nested = helper.getFixture('nested.json');
    db.jsonld.put(nested, function(err, obj) {
      console.log(obj)
      db.jsonld.get({ '@id': obj["@id"], '@context': obj['@context'] }, function(err, result) {
        console.log(result)
        db.get({}, console.log)
        delete result['knows'][0]['@id'];
        delete result['knows'][1]['@id'];
        expect(result).to.eql(nested);
        done();
      });
    });
  });

  it('with an object with multiple objects for same predicate' ,function(done){
    var bbb = helper.getFixture('bigbuckbunny.json');

    var act1 = {
          subject: bbb['@id'],
          predicate: 'http://schema.org/actor',
          object: 'http://example.net/act1'
    };

    var act2 = {
          subject: bbb['@id'],
          predicate: 'http://schema.org/actor',
          object: 'http://example.net/act2'
    };

    db.jsonld.put(bbb, function() {
      db.put([act1, act2], function() {
        db.jsonld.get(bbb['@id'], bbb['@context'], function(err, doc) {
          expect(doc['actor']).to.be.an('array');
          expect(doc['actor']).to.have.length(2);
          done();
        });
      });
    });
  });
});

describe('with an object with an array for its ["@type"]', function() {
  var db, ratatat;

  beforeEach(function(done) {
    ratatat = helper.getFixture('ratatat.json');
    db = helper.getDB({ jsonld: { base: 'http://levelgraph.io/get' } });
    db.jsonld.put(ratatat, done);
  });

  it('should retrieve the object', function(done) {
    db.jsonld.get(ratatat['@id'], {}, function(err, obj) {
      expect(obj['@type']).to.have.members(ratatat['@type']);
      expect(obj['@id']).to.eql(ratatat['@id']);
      done();
    });
  });
});


describe('with frames', function() {
  var db, library;

  beforeEach(function(done) {
    library = helper.getFixture('library.json');
    db = helper.getDB({ jsonld: { base: 'http://levelgraph.io/get' } });
    db.jsonld.put(library, done);
  });

  it('should respect @embed', function(done) {
    db.jsonld.get({
      "@context": {
        "dc": "http://purl.org/dc/elements/1.1/",
        "ex": {
          "@id": "http://example.org/vocab#"}
      },
      "@id": "http://example.org/library",
      "@type": "ex:Library",
      "ex:contains": {
        "@type": "ex:Book",
        "@embed": "@never"
      }
    }, function(err, obj) {
      expect(obj["ex:contains"]["@id"]).to.eql("http://example.org/library/the-republic");
      expect(obj["ex:contains"]["@type"]).to.be.empty;
      done();
    });
  });

  it('should respect @embed rapidly', function(done) {
    console.time('createdeep')

    var deep = Array.from({ length: 10000 }, function (v,k) { return {
      "@id": `${k}`,
      "value": `${k}`,
      "link": `${k+1}`
    }})

    console.timeEnd('createdeep')
    console.time('put')

    db.jsonld.put({
        "@context": {
          "link": {
            "@id": "http://example.org/link#",
            "@type": "@id"
          },
          "@base": "https://levelgraph.io/get/",
          "@vocab": "http://example.org/vocab#"
        },
        "@graph": deep
      }, function (err, obj) {
        console.timeEnd('put')
      // console.log("obj")
      // console.log(JSON.stringify(obj,true,2))
      console.time('get')
      db.jsonld.get({
        "@context": {
          "link": {
            "@id": "http://example.org/link#",
            "@type": "@id"
          },
          "@base": "https://levelgraph.io/get/",
          "@vocab": "http://example.org/vocab#"
        },
        "@id": "0",
        "value": {},
        "link": {
          "@id": "1",
          "@embed": "@never"
        }
      }, function(err, obj) {
        console.timeEnd('get')
        console.log("get result");
        console.log(JSON.stringify(obj,true,2));
        expect(obj["link"]).to.eql("1");
        expect(obj["value"]).to.eql("0");
        done();
      });
    });
  });

  it('should get a deep object', function(done) {
    console.time('createdeep')

    var deep = Array.from({ length: 100 }, function (v,k) { return {
      "@id": `${k}`,
      "value": `${k}`,
      "link": `${k+1}`
    }})

    console.timeEnd('createdeep')
    console.time('put')

    db.jsonld.put({
        "@context": {
          "link": {
            "@id": "http://example.org/link#",
            "@type": "@id"
          },
          "@base": "https://levelgraph.io/get/",
          "@vocab": "http://example.org/vocab#"
        },
        "@graph": deep
      }, function (err, obj) {
        console.timeEnd('put')
      // console.log("obj")
      // console.log(JSON.stringify(obj,true,2))
      console.time('get')
      db.jsonld.get({
        "@context": {
          "link": {
            "@id": "http://example.org/link#",
            "@type": "@id"
          },
          "@base": "https://levelgraph.io/get/",
          "@vocab": "http://example.org/vocab#"
        },
        "@id": "0",
        "value": {},
        "@embed": "@always"
      }, function(err, obj) {
        console.timeEnd('get')
        // console.log("get result");
        // console.log(JSON.stringify(obj,true,2));
        expect(obj["link"]["@id"]).to.eql("1");
        expect(obj["value"]).to.eql("0");
        done();
      });
    });
  });

  it('should respect @embed and strange framing yielding null link', function(done) {
    var deep = Array.from({ length: 100 }, function (v,k) { return {
      "@id": `${k}`,
      "value": `${k}`,
      "link": `${k+1}`
    }})

    db.jsonld.put({
        "@context": {
          "link": {
            "@id": "http://example.org/link#",
            "@type": "@id"
          },
          "@base": "https://levelgraph.io/get/",
          "@vocab": "http://example.org/vocab#"
        },
        "@graph": deep
      }, function (err, obj) {
      // console.log("obj")
      // console.log(JSON.stringify(obj,true,2))
      db.jsonld.get({
        "@context": {
          "link": {
            "@id": "http://example.org/link#",
            "@type": "@id"
          },
          "@base": "https://levelgraph.io/get/",
          "@vocab": "http://example.org/vocab#"
        },
        "@id": "0",
        "value": {},
        "link": {
          "@id": "1",
          "@embed": "@never",
          "link": {
            "@id": "2"
          }
        }
      }, function(err, obj) {
        console.log("get result");
        console.log(JSON.stringify(obj,true,2));
        db.get({}, function(err, triples) {
          // console.log(triples);
          done() })
      });
    });
  });

});

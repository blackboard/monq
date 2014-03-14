var assert = require('assert');
var helpers = require('./helpers');
var Queue = require('../lib/queue');
var sinon = require('sinon');
var Worker = require('../lib/worker');

describe('job', function() {
    var queue, handler, worker, failHandler;

    before(function(done) {
        queue = new Queue({ db: helpers.db });

        handler = sinon.spy(
            function(params, callback){
                return callback(new Error());
            }
        );

        failHandler = sinon.spy();

        worker = new Worker([ queue ], { interval: 10 });
        worker.register({ retry: handler });
        worker.start();

        worker.on('failed', failHandler);

        queue.collection.remove({}, done);
    });

    after(function(done){
        worker.stop(done);
    });

    after(function(done){
        queue.collection.remove({}, done);
    });

    describe('with retries', function(){
        it('enqueues', function(done){
            queue.enqueue('retry', { test: 'data' }, { attempts: { count: 3 } }, done);
        });

        it('calls the handler 3 times', function(done){
            (function hasFinished(){
                if(handler.calledThrice){
                    done();
                } else {
                    setTimeout(hasFinished, 10);
                }
            })();
        });

        it('emits failed 3 times', function(){
            assert.ok(failHandler.calledThrice);
        });

        it('updates the job status', function(){
            var job = failHandler.lastCall.args[0];

            assert.equal(job.attempts.attemptsLeft, 0);
            assert.equal(job.attempts.count, 3);
            assert.equal(job.status, 'failed');
        });
    });
});

describe('job', function() {
    var queue, handler, startTime, worker, failHandler;

    before(function(done) {
        queue = new Queue({ db: helpers.db });

        handler = sinon.spy(
            function(params, callback){
                return callback(new Error());
            }
        );

        failHandler = sinon.spy();

        worker = new Worker([ queue ], { interval: 10 });
        worker.register({ retry: handler });
        worker.start();

        worker.on('failed', failHandler);

        queue.collection.remove({}, done);
    });

    after(function(done){
        worker.stop(done);
    });

    after(function(done) {
        queue.collection.remove({}, done);
    });

    describe('with retries and retry delay', function(){
        it('enqueues', function(done){
            startTime = new Date();
            queue.enqueue('retry', { test: 'data' }, { attempts: { count: 3, delayMS: 50 } }, done);
        });

        it('calls the handler 3 times', function(done){
            (function hasFinished(){
                if(handler.calledThrice){
                    done();
                } else {
                    setTimeout(hasFinished, 10);
                }
            })();
        });

        it('takes longer than 100ms', function(){
            assert.ok(new Date().getTime() - startTime.getTime() > 100);
        });

        it('emits failed 3 times', function(){
            assert.ok(failHandler.calledThrice);
        });

        it('updates the job status', function(){
            var job = failHandler.lastCall.args[0];
            assert.equal(job.attempts.attemptsLeft, 0);
            assert.equal(job.attempts.count, 3);
            assert.equal(job.status, 'failed');
        });
    });
});

describe('job', function() {
    var queue, handler, startTime, worker, failHandler;

    before(function(done) {
        queue = new Queue({ db: helpers.db });

        handler = sinon.spy(
            function(params, callback){
                return callback(new Error());
            }
        );

        failHandler = sinon.spy();

        worker = new Worker([ queue ], { interval: 10 });
        worker.register({ retry: handler });
        worker.start();

        worker.on('failed', failHandler);

        queue.collection.remove({}, done);
    });

    after(function(done){
        worker.stop(done);
    });

    after(function(done) {
        queue.collection.remove({}, done);
    });

    describe('with retries and exponential strategy', function(){
        it('enqueues', function(done){
            startTime = new Date();
            queue.enqueue(
                'retry',

                { test: 'exponential' },

                {
                    attempts: {
                        count: 5,
                        delayStrategy: 'exponential',
                        delayMS: 10
                    }
                },

                done
            );
        });

        it('calls the handler 3 times', function(done){
            (function hasFinished(){
                if(handler.callCount === 5){
                    done();
                } else {
                    setTimeout(hasFinished, 10);
                }
            })();
        });

        it('takes longer than 100ms', function(){
            assert.ok(new Date().getTime() - startTime.getTime() > 100);
        });

        it('emits failed 5 times', function(){
            assert.equal(failHandler.callCount, 5);
        });

        it('updates the job status', function(){
            var job = failHandler.lastCall.args[0];
            assert.equal(job.attempts.attemptsLeft, 0);
            assert.equal(job.attempts.count, 5);
            assert.equal(job.status, 'failed');
        });
    });
});

describe('job', function() {
    var queue, handler, worker, failHandler;

    before(function(done) {
        queue = new Queue({ db: helpers.db });

        handler = sinon.spy(
            function(params, callback){
                return callback(new Error());
            }
        );

        failHandler = sinon.spy();

        worker = new Worker([ queue ], { interval: 10 });
        worker.register({ retry: handler });
        worker.start();

        worker.on('failed', failHandler);

        queue.collection.remove({}, done);
    });

    after(function(done){
        worker.stop(done);
    });

    after(function(done){
        queue.collection.remove({}, done);
    });

    describe('without retries', function(){
        it('enqueues', function(done){
            queue.enqueue('retry', { test: 'data' }, done);
        });

        it('calls the handler only once', function(done){
            setTimeout(function(){
                assert.ok(handler.calledOnce);
                done();
            }, 50); //Give it time to poll a few times, if it was going to
        });

        it('emits failed once', function(){
            assert.ok(failHandler.calledOnce);
        });

        it('updates the job status', function(){
            var job = failHandler.lastCall.args[0];
            assert.equal(job.attempts, undefined);
            assert.equal(job.status, 'failed');
        });
    });
});
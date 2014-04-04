//The watchdog handles jobs that have timed out due to process crashes or other catastrophic failures

var events = require('events');
var util = require('util');

function WatchDog(queue, options){
    this.queue = queue;
    this.stopped = true;

    options = options || {};
    this.options = {
        interval: options.interval || 60000,
        gracePeriod: options.gracePeriod || 10000 //Additional time to wait before timing out a job with the watch dog
    }
}

util.inherits(WatchDog, events.EventEmitter);

var _getTimedOutJob = function(queue, gracePeriod, callback){
    var query = {
        status: 'dequeued',
        queue: queue.name,

        //Give the worker ample time to process the failure itself first to avoid a race condition
        timeoutAt: { $lt: new Date(new Date().getTime() + gracePeriod) }
    };
    var sort = { timeoutAt: 1 };
    var options = { new: true };

    //We only set the status to timeout temporarily to prevent the race condition of multiple processes timing out the same job.
    //The job will either be failed or retried.
    var update = { $set: { status: 'timedout' } };

    queue.collection.findAndModify(query, sort, update, options, function(err, doc){
        callback(err, doc ? queue.job(doc) : doc);
    });
};

WatchDog.prototype.start = function(){
    var self = this;
    this.stopped = false;

    var cleanup = function(){
        _getTimedOutJob(self.queue, self.options.gracePeriod, function(err, job){
            if(self.stopped){
                return;
            }

            if(err){
                self.emit(err); //We do want to schedule the timer again even if an error occurs
                scheduleTimer();
            } else if(job){
                job.fail(new Error('Timed out'), function(err, job){
                    if(err){
                        scheduleTimer();
                        return self.emit(err);
                    }

                    self.emit('timedout', job.data);

                    //If a job to timeout was found, execute cleanup again immediately to more quickly work
                    //through a potential backlog of timed out tasks
                    if(!self.stopped){
                        cleanup();
                    }
                });
            } else {
                scheduleTimer();
            }
        });
    };

    var scheduleTimer = function(){
        self.timer = setTimeout(cleanup, self.options.interval);
    };

    //For the first run of the watchdog, use a random delay. This will stagger multiple watchdogs starting at the same time.
    self.timer = setTimeout(cleanup, Math.floor(Math.random() * self.options.interval));
};

WatchDog.prototype.stop = function(){
    this.stopped = true;
    clearTimeout(this.timer);
};

module.exports = WatchDog;
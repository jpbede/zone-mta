// NB! these counters do not expire so it might be too simplistic
class MTALocker {
    constructor() {
        this.counters = new Map();
    }

    // helper to get correct counter Map
    getPidCounters(pid){
        if(!this.counters.has(pid)){
            this.counters.set(pid, new Map());
        }
        return this.counters.get(pid);
    }

    // call when child process dies
    clearPidCounters(pid){
        this.counters.delete(pid);
    }

    lock(pid, key) {
        let counters = this.getPidCounters(pid);
        if(!counters.has(key)){
            counters.set(key, 1);
        }else{
            counters.set(key, counters.get(key) + 1);
        }
    }

    release(pid, key) {
        let counters = this.getPidCounters(pid);
        if(!counters.has(key)){
            return;
        }

        counters.set(key, counters.get(key) - 1);
        if(counters.get(key) <= 0){
            // delete the entry is counter value is too small
            counters.delete(key);
        }
    }

    isFree(key, maxConnections) {
        let sum = 0;
        this.counters.forEach(counters=>{
            if(counters.has(key)){
                sum += counters.get(key);
            }
        })
        return sum < maxConnections;
    }
}

module.exports = MTALocker;
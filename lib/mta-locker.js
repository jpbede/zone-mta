const log = require('npmlog');
const config = require('wild-config');

class MTALocker {
    constructor() {
        this.counters = new Map();

        // precompile regex
        this.precompiledRegex = [];

        let precompileRegex = () => {
            let regexConfig = config.mxConfig;
            for (let regKey in regexConfig) {
                this.precompiledRegex.push({
                    regexp: new RegExp(regKey),
                    config: regexConfig[regKey]
                });
            }
            log.verbose("MTALocker", "Keys precompiled");
        }
        precompileRegex();

        config.on('reload', () => {
            log.verbose("MTALocker", "Reloading MTA configs");
            this.precompiledRegex = [];
            precompileRegex();
        });
    }

    _getMXConfig(mx) {
        if (this.precompiledRegex.length > 0) {
            for (let pRegex of this.precompiledRegex) {
                if (pRegex.regexp.test(mx)) {
                    return pRegex.config;
                }
            }
        }

        return config.mxConfig.default;
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
        log.verbose("MTALocker", "MTA locked with '"+key+"'");
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
        log.verbose("MTALocker", "MTA released with '"+key+"'");
    }

    isFree(key, mx) {
        let config = this._getMXConfig(mx);
        let sum = 0;
        this.counters.forEach(counters=>{
            if(counters.has(key)){
                sum += counters.get(key);
            }
        })
        return sum < config.maxConnections;
    }
}

module.exports = MTALocker;
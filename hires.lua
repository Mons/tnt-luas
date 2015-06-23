local ffi = require("ffi")
ffi.cdef[[
struct timeval {
	uint64_t      tv_sec;
	uint64_t      tv_usec;
};

struct timespec {
	uint64_t   tv_sec;
	long       tv_nsec;
};

uint64_t time(uint64_t *t);
int gettimeofday(struct timeval *tv, struct timezone *tz);
int clock_gettime(int clk_id, struct timespec *tp);
]]

timeval = ffi.typeof("struct timeval");
timespec = ffi.typeof("struct timespec");

local CLOCK_REALTIME = 0
local CLOCK_MONOTONIC = 1
local CLOCK_PROCESS_CPUTIME_ID = 2
local CLOCK_THREAD_CPUTIME_ID = 3
local CLOCK_MONOTONIC_RAW = 4
local CLOCK_REALTIME_COARSE = 5
local CLOCK_MONOTONIC_COARSE = 6

local C = ffi.C

local function clock()
	local ts = timespec()
	local x = C.clock_gettime(CLOCK_THREAD_CPUTIME_ID,ts)
	return tonumber(ts.tv_sec) + tonumber(ts.tv_nsec)/1e9;
end

local function hitime()
	local tv = timeval();
	C.gettimeofday(tv,nil);
	return tonumber(tv.tv_sec) + tonumber(tv.tv_usec)/1e6;
end

return {
	time = hitime;
	clock = clock;
}

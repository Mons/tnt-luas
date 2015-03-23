local ffi = require 'ffi'
ffi.cdef [[
void _exit(int status);
]]

local io = require 'io'
local seq = 0

local good = 0
local bad = 0
local planned

local p

if rawget(_G, 'p') and type(_G.p) == 'function' then
	p = _G.p
else
	p = function (f,...) io.stdout:write(string.format(f,...)) end
end

local _print = print
function print(...)
	local t = {...}
	for _,v in ipairs(t) do
		t[_] = tostring(v)
	end
	p("# %s\n",table.concat(t, ""))
end


local function _out(test,name,diags)
	seq = seq + 1
	if test then good = good + 1 else bad = bad + 1 end
	
	local out = string.format("%sok %d%s\n",
		not test and "not " or "",
		seq,
		name and " - "..name or ""
	)
	p(out)
	if not test then
		local caller = debug.getinfo(3)
		p("#   Failed test '%s'\n",name)
		p("#   at %s:%d\n", caller.short_src, caller.currentline)
		if diags then
			for _,v in pairs(diags) do
				p("#    %s\n",v)
			end
		end
	end
	
end

function plan(num)
	if seq == 0 then
		planned = num
		p("%d..%d\n",1,num)
	else
		error("Can't plan after test")
	end
end

function ok(test,name)
	_out(test,name)
	return test
end

function is(got,need,name)
	local test = got == need
	_out(test,name,test and nil or {
		string.format(" got: '%s'",tostring(got)),
		string.format("need: '%s'",tostring(need)),
	});
	return test
end

function isnt(got,need,name)
	local test = got ~= need
	_out(test,name,test and nil or {
		string.format(" got: '%s'",tostring(got)),
		string.format("need: anything else"),
	});
	return test
end

function like(got,pat,name)
	local test = string.match(got,pat) ~= nil
	_out(test,name,test and nil or {
		string.format("      got: '%s'",tostring(got)),
		string.format("not match: '%s'",tostring(pat)),
	});
	return test
end

function unlike(got,pat,name)
	local test = string.match(got,pat) == nil
	_out(test,name,test and nil or {
		string.format("  got: '%s'",tostring(got)),
		string.format("match: '%s' but shouldn't",tostring(pat)),
	});
	return test
end

function done_testing()
	if not planned then
		p("%d..%d\n",1,seq)
	end
	if seq == 0 then
		p("# No tests run!\n")
		ffi.C._exit( 255 )
	end
	if planned then
		if planned ~= seq then
			_out(false,string.format("planned to run %d but done_testing() expects %d",planned,seq))
			p("# Looks like you planned %d tests but ran %d.\n",planned,seq-1)
		end
	end
	if bad > 0 then
		p("# Looks like you failed %d test of %d.\n",bad,seq)
		ffi.C._exit( 255 )
	end
	io.flush()
	ffi.C._exit( 0 )
end

local function clone_table(t)
	local newt = {}
	for k,v in pairs(t) do
		if type(v) == 'table' then
			newt[k] = clone_table(v)
		else
			newt[k] = v
		end
	end
	return newt
end

local function _compare_table(got,need)
	local test = true
	for nk,nv in pairs(need) do
		if got[nk] then
			if type(nv) == 'table' then
				test = _compare_table(got[nk],nv)
			else
				test = got[nk] == nv
			end
		else
			test = false
		end
		if not test then break end
	end
	return test
end

function is_deeply(got,need,name)
	local path = {}
	local err
	local function _cmp(a, b, rev)
		local test = true
		if type(a) ~= 'table' or type(b) ~= 'table' then
			err = {
				'expected: '..dumper(b),
				'     got: '..dumper(a),
			}
			return false
		end
		for nk,nv in pairs(b) do
			if a[nk] ~= nil then
				if type(nv) == 'table' then
					table.insert(path, 1, nk)
					test = _cmp(a[nk], nv, rev)
					if test then
						table.remove(path, 1)
					end
				else
					test = a[nk] == nv
				end
			else
				test = false
			end
			if not test then
				if not err then
					local p = ''
					for _ = #path, 1, -1 do p = p .. path[_] .. '.' end
					if rev then
						err = { 'expected.' .. p .. nk .. ' = ' .. dumper(a[nk]), '     got.' .. p .. nk .. ' = ' .. dumper(b[nk]) }
					else
						err = { 'expected.' .. p .. nk .. ' = ' .. dumper(b[nk]), '     got.' .. p .. nk .. ' = ' .. dumper(a[nk]) }
					end
					err[3] = '-----'
					err[4] = '     got: ' .. dumper(got)
					err[5] = 'expected: ' .. dumper(need)
				end
				return false
			end
		end
		return test
	end
	
	local test = _cmp(clone_table(got),clone_table(need)) and _cmp(clone_table(need),clone_table(got), true)
	_out(test, name, err);
	return test
end

local _dump_seen = {}
local function _dumper(t)
	if type(t) == 'table' then
		
		if _dump_seen[ tostring(t) ] then
			return '\\'..tostring(t)
		end
		_dump_seen[ tostring(t) ] = true
		
		local keys = 0
		for _,_ in pairs(t) do keys = keys + 1 end
		if keys ~= #t then
			local sub = {}
			local prev = 0
			for k,v in pairs(t) do
				if type(k) == 'number' and k == prev + 1 then
					prev = k
					table.insert(sub,_dumper(v))
				else
					table.insert(sub,tostring(k)..'='.._dumper(v))
				end
			end
			return '{'..table.concat(sub,'; ')..'}'
		else
			local sub = {}
			for _,v in ipairs(t) do
				table.insert(sub,_dumper(v))
			end
			return '{'..table.concat(sub,', ')..'}'
		end
	elseif type(t) == 'number' then
		return tostring(t)
	elseif type(t) == 'string' then
		return "'" .. t .. "'"
	else
		return tostring(t)
	end
end
function dumper(x)
	local r = _dumper(x)
	_dump_seen = {}
	return r
end

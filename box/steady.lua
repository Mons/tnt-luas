-- for good lua reloading

--[[ -- TODO
if box.info.version > "1.5.4-57" then
	function box.sandbox()
		os.execute = nil
		os.exit = nil
		os.rename = nil
		os.tmpname = nil
		os.remove = nil
		io = nil
		require = nil
		package = nil
	end
end
]]

if not rawget(_G,'bkp') then rawset(_G,'bkp',{_first = true, gen = 0}) end
if bkp._first then
	local loaded = {}
	bkp.loaded = {}
	for m in pairs(package.loaded) do bkp.loaded[m] = package.loaded[m] end
	
	bkp.insert = box.insert
	bkp.update = box.update
	bkp.delete = box.delete
	bkp.select = box.select
	bkp.replace = box.replace
	bkp.raise  = box.raise
	bkp.error  = error
	
	bkp.package = package
	bkp.require = require
	bkp.first   = true
	bkp._first  = false
	bkp.fibers  = {}
	
	print("Loading lua 1st time, loaded: "..table.concat(loaded, ", "))
	
	if rawget(_G,'LIBDIR') then
		package.path = package.path .. ';'..LIBDIR..'/?.lua'
	end
	
	require 'box.reload'
	local ctr = require 'box.conntrack'
	function bkp:connect()
		if box.fiber.is_admin() then
			rawset(_G,'package',bkp.package)
			rawset(_G,'require',bkp.require)
		end
	end
	ctr.on_connect(bkp,bkp.connect)
	
	-- useful extensions
	rawset(_G,'NULL',box.cjson.decode( 'null'))
	rawset(_G,'FIRST',true)
	
	function string.tohex(str)
		local r = str:gsub('.', function (c) return string.format('%02X', string.byte(c)) end)
		return '\\x'..r
	end
	
	function string.xd(str)
		local r = str:gsub('.', function (c) return string.format('%02X ', string.byte(c)) end)
		return r
	end
	
	function dots2hash(...)
		local r = {}
		local data = {...}
		for i = 1, #data, 2 do
			r[ data[ i ] ] = data[ i + 1 ]
		end
		return r
	end
	
	if box.time64 == nil then
		box.time64 = function()
			return tonumber64(os.time()*(1e6))
		end
	end
	
	function L(x) return box.unpack('l',x) end
	function I(x) return box.unpack('i',x) end
	
	--[[
	-- deadly fibers
	do
		local ffi = require 'ffi'
		
		ffi.cdef ' typedef struct { long fid; } fiber_guard_t; '
		local guard = ffi.metatype('fiber_guard_t', {
			__gc = function( x )
				local fid = tonumber(x.fid)
				local f = box.fiber.find( fid )
				if f then
					box.fiber.cancel(f)
				end
			end
		})
		
		function fiber(fn, name, ...)
			local f = box.fiber.create(function (...)
				if name then box.fiber.name(name) end
				box.fiber.detach()
				fn(...)
			end)
			box.fiber.resume(f,...)
			return setmetatable({
				__guard = guard({ box.fiber.id(f) })
			},{
				__index = f
			})
		end
	end
	]]
else
	bkp.first = false
	bkp.gen = bkp.gen + 1
	rawset(_G,'package',bkp.package)
	rawset(_G,'require',bkp.require)
	rawset(_G,'FIRST',false)
	local reloads = {}
	for m in pairs(package.loaded) do
		if not bkp.loaded[m] then
			package.loaded[m] = nil
			table.insert(reloads,m)
		end
	end
	print("Reloading lua (",bkp.gen,"), mods: "..table.concat(reloads,", "))
	require 'box.reload'
	require 'box.conntrack'
	box.reload:cleanup()
end


local caller = require 'devel.caller'

box.steady = box.steady or {}

function box.steady.intercept_errors(on,printstack)
	if on then
		function box.delete(...)
			local r,e = pcall(bkp.delete,...)
			if not r then
				local clr = caller(1)
				box.fiber.wrap(function(args) print("catch: delete ",box.tuple.new(args),": ",e,clr) end, {...})
				box.raise(11,e)
			end
			return e
		end
		
		function box.insert(...)
			local r,e = pcall(bkp.insert,...)
			if not r then
				local clr = caller(1)
				box.fiber.wrap(function(args) print("catch: insert ",box.tuple.new(args),": ",e,clr) end, {...})
				box.raise(11,e)
			end
			return e
		end
		
		function box.replace(...)
			local r,e = pcall(bkp.replace,...)
			if not r then
				local clr = caller(1)
				box.fiber.wrap(function(args) print("catch: replace ",box.tuple.new(args),": ",e,clr) end, {...})
				box.raise(11,e)
			end
			return e
		end
		
		function box.update(...)
			local r,e = pcall(bkp.update,...)
			if not r then
				local clr = caller(1)
				box.fiber.wrap(function(args) print("catch: update ",box.tuple.new(args),": ",e,clr) end, {...})
				box.raise(11,e)
			end
			return e
		end

		function box.select(...)
			local r = { pcall(bkp.select,...) }
			if not r[1] then
				local clr = caller(1)
				box.fiber.wrap(function(args) print("catch: select ",box.tuple.new(args),": ",r[2],clr) end, {...})
				box.raise(11,r[2])
			end
			table.remove(r,1)
			return unpack(r)
		end
		
		rawset(_G,'error',function(e,f)
			f = f or 1
			local clr = caller(f)
			local caller = debug.getinfo(1+f)
			local msg = ( caller and caller.short_src .. ':'..caller.currentline or '' )..': '..tostring(e)
			
			local stack = {}
			if printstack then
				local st = 1
				while true do
					st = st + 1
					local c = debug.getinfo(st)
					if not c then break end
					table.insert(stack,"\t@ "..c.short_src .. ':'..c.currentline..( c.name and ':'..c.name..'()' or '') )
				end
			end
			
			box.fiber.wrap(function()
				print("error :'",e,"'",clr)
				if #stack > 0 then
					for _,v in ipairs(stack) do
						print(v)
					end
				end
			end)
			bkp.raise(51,msg)
		end)
	else
		box.delete = bkp.delete
		box.insert = bkp.insert
		box.replace = bkp.replace
		box.update = bkp.update
		box.select = bkp.select
		
		rawset(_G,'error',bkp.error)
	end
end

setmetatable(box.steady,{
	__index = { gen = bkp.gen },
	__newindex = function (t,k,v)
		error("Readonly "..tostring(k).." -> "..tostring(v),2)
	end
})

package.loaded['box.steady'] = box.steady

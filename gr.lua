local log = require 'box.log'

local K = ...

local tcp = require( K..'.tcp' )
local udp = require( K..'.udp' )

local old = rawget(_G,K)

if old then
	-- todo signal old to shutdown
	for k,i in pairs(old) do
		if type(i) == 'table' then
			print("got key ",k," ",old[k])
			if i._c and i._c.fihish then
				i._c:finish()
			end
		end
	end
end

local function new ( name )
	return {
		_name   = name,
		_c      = false,
		_warned = false,
	}
end

local M = new('main')

-- M.__index = M

function M:config(host,port,proto)
	assert(type(self) == 'table', "Static call prohibited")
	if proto == 'tcp' then
		local cnn = tcp(host,port)
		self._c = cnn
	elseif proto == 'udp' then
		local cnn = udp(host,port)
		self._c = cnn
	else
		error("Bad protocol "..proto, 2)
	end
end

function M:send(key,value)
	if not self._c then
		if not self._warned then
			self._warned = true
			log._log( log.WARN, "Instance `%s' not configured prematurely", self._name )
		end
		return
	end
	self._c:send(key, value)
end

setmetatable(M,{
	__index = function(t,k)
		print("Creating new instance ",k)
		local newobj = setmetatable(
			new(k),
			{
				__index = t,
				-- __newindex = function() error("",2) end
			}
		)
		rawset(t,k,newobj)
		return newobj
	end,
	__newindex = function(t,k,v)
		print("new index ",k)
		error("Prohibited",2)
	end
})

rawset(_G,K,M)

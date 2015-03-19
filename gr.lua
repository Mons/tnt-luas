local log = require 'box.log'

local K = ...

local tcp = require( K..'.tcp' )
local udp = require( K..'.udp' )

local old = rawget(_G,K)

if old then
	-- todo signal old to shutdown
	
end

local function new ( name )
	return {
		name   = name,
		ok     = false,
		c      = false,
		warned = false,
	}
end

local M = new('main')

-- M.__index = M

function M:config(host,port,proto)
	assert(type(self) == 'table', "Static call prohibited")
	print("called on ",self," name=",self.name," param = ",param)
	if proto == 'tcp' then
		local cnn = tcp(host,port)
		self.c = cnn
	elseif proto == 'udp' then
		local cnn = udp(host,port)
		self.c = cnn
	else
		error("Bad protocol "..proto, 2)
	end
end

function M:send(key,value)
	if not self.c then
		if not self.warned then
			self.warned = true
			log._log( log.WARN, "Instance `%s' not configured prematurely", self.name )
		end
		return
	end
	self.c:send(key, value)
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

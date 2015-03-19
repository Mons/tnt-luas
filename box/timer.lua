local log = require 'box.log'

local old = rawget(_G,'box.timer')

-- todo respawn...

local M = setmetatable(
	{
		periodics = old and old.periodics or {}
	},
	{
		__call = function(cls, ...)
			print("call on ",cls, ...)
			return cls:new(...)
		end,
		-- __newindex = function() error("not allowed",2) end,
	}
)

M.__index = M

function M.periodic(self,name,int,func)
	if type(self) ~= 'table' then
		name,int,func = self,name,int
		self = M
	end
	local p = { work = true }
	if self.periodics[ int ] then
		self.periodics[ int ].work = false
	end
	self.periodics[ int ] = p
	p.f = box.fiber.wrap(function()
		box.fiber.name("periodic."..name)
		local start = box.time()
		box.fiber.sleep(  math.ceil(start) - start - 0.001 )
		start = math.ceil(start)
		while p.work do
			local nxt = start + int
			start = nxt
			local r,e = pcall(func)
			if not r then log.err("call periodic failed: %s",e) end
			box.fiber.sleep(0) -- refresh time
			box.fiber.sleep(nxt - box.time())
		end
		print("gone")
	end)
end

box.timer = M
rawset(_G,'box.timer',M)

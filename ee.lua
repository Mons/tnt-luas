local object = require 'box.oop'

local M = object:define({
	__name = 'ee',
	handlers = {},
	bycb    = {},
})

function M:on(event,cb)
	local rec = { cb = cb }
	return self:_on(event,rec)
end

function M:once(event,cb)
	local rec = { cb = cb, limit = 1 }
	return self:_on(event,rec)
end

function M:_on(event,rec)
	if self.handlers[event] then
		table.insert(self.handlers[event].chain,1,rec)
	else
		local h = { chain = { rec }, event = event }
		self.handlers[event] = h
		h.ch = box.ipc.channel(1000)
		h.working = true
		h.fiber = box.fiber.wrap(function()
			box.fiber.name('ee.'..event)
			while #h.chain > 0 do
				local event = h.ch:get(1)
				if event then
					print("received event in fiber ",unpack(event))
					print(#h.chain)
					h.handled = false
					local c = {}
					c.stop = function() h.handled = true end
					local rem = {}
					for i,rec in ipairs(h.chain) do
						print("apply event for ",i," ",rec.cb)
						local r,e = pcall(rec.cb,c,unpack(event))
						if not r then
							print("callback failed: ",e)
						end
						if rec.limit then
							rec.limit = rec.limit - 1
							if rec.limit == 0 then
								table.insert(rem,1,i)
								self:no(rec.cb)
							end
						end
						if h.handled then break end
					end
					if #rem > 0 then
						for _,i in ipairs(rem) do
							local rec = table.remove( h.chain, i )
							self.bycb[ rec.cb ] = nil
						end
					end
					h.handled = nil
				end
			end
			print("fiber gone")
			h.fiber = nil
			h.ch:close()
			self.handlers[event] = nil
		end)
	end
	self.bycb[ rec.cb ] = self.handlers[event]
	return function()
		self:no( rec.cb )
	end
end

function M:no(arg)
	if type(arg) == 'function' then
		print("unreg ",arg)
		if self.bycb[ arg ] then
			local h = self.bycb[ arg ]
			print("unregistering cb for ",h.event)
			for i,rec in ipairs(h.chain) do
				if rec.cb == arg then
					table.remove( h.chain, i )
					self.bycb[ rec.cb ] = nil
					break
				end
			end
		else
			print("have no listener "..tostring(arg).." anymore")
		end
	end
end

function M:event(event, ...)
	if not self.handlers[event] then return end
	print("dispatch event ",event)
	self.handlers[event].ch:put({...})
end

function M:handles(event)
	if not self.handlers[event] then return false end
	return #self.handlers[event]
end

M.emit = M.event

local O = M()
rawset(_G,'ee',O)

return O

local object  = require 'box.oop'
local log     = require 'box.log'
local rst     = require 'box.rst'

local R = setmetatable({}, { __mode = "kv" })

local M = object:define({ __name = 'eq' })
M.DEF_PRI = 0x7fff

--[[
box.fiber.wrap(function()
	box.fiber.name("eq:gc")
	while true do
		collectgarbage()
		for k,v in pairs(R) do
			print(k,": ",v)
		end
		box.fiber.sleep(1)
	end
end)
]]

--[[
	1. Queue required index on [status, prio]
]]

function M:on_disconnect(sid)
	print(self, " received disconnect for ",sid)
	if self.bysid[sid] then
		for kfi in pairs(self.bysid[sid]) do
			self.taken[kfi] = nil
			local kf = {self:keyunpack(kfi)}
			print("rm ", box.tuple.new(kf))
			local t = box.select(self.space,0,kf)
			if t then
				if t[ self.f_stt ] == 'T' then
					t = box.update(self.space, kf, '=p', self.f_stt, 'R' )
					print("returned ", t, " to ",t[self.f_stt] )
					self:wakeup(t)
				else
					print("rm ", box.tuple.new(kf), " -> ", t," have wrong status: ",t[self.f_stt] )
				end
			else
				print("found no record for ",box.tuple.new(kf),", was marked as taken by ",sid)
			end
		end
		self.bysid[sid] = nil
	end
	
end

function M:init(opts)
	R[self.__id] = self
	
	rst:register( self, self.on_disconnect )
	
	self.space = opts.space
	self.index = opts.index
	self.zombie = opts.zombie
	
	-- self.pri   = opts.pri or self.DEF_PRI
	if opts.f_id then
		self.f_id = opts.f_id
		self.auto_id = true
	end
	
	self.f_stt = opts.f_stt or 0
	self.f_pri = opts.f_pri
	self.f_cnt = opts.f_cnt
	self.f_cnb = opts.f_cnb
	self.f_runat = opts.f_runat
	self.auto_increment = opts.auto_increment
	
	self.def = {
		f_stt = 'R',
		f_pri = opts.pri or self.DEF_PRI,
	}
	self.wseq = 0
	self.wait = {}
	self.taken = {}
	self.bysid = {}
	
	
	assert(box.space[self.space],"unknown space "..self.space)
	
	if not self.index then
		for idx,index in pairs(box.space[self.space].index) do
			if index.key_field[0] and index.key_field[0].fieldno == self.f_stt then
				if self.f_pri then
					if index.key_field[1] and index.key_field[1].fieldno == self.f_pri then
						print("match index ",idx," for status and pri")
						assert(not self.index, "Ambiguous decision on index: "..tostring(self.index).." vs "..tostring(idx))
						self.index = idx
					end
				else
					print("match index ",idx," for status")
					assert(not self.index, "Ambiguous decision on index: "..tostring(self.index).." vs "..tostring(idx))
					self.index = idx
				end
			end
		end
	end
	assert(box.space[self.space].index[self.index],"unknown index "..self.index.." in space "..self.space)
	if self.f_runat then
		for idx,index in pairs(box.space[self.space].index) do
			if index.key_field[0] and index.key_field[0].fieldno == self.f_runat then
				print("match index ",idx," for runat")
				self.index_runat = idx
				break
			end
		end
	elseif self.zombie then
		print(".zombie is useless without runat")
		self.zombie = nil
	end
	
	-- print("kf = ",#box.space[self.space].index[self.index].key_field)
	-- if true then return end
	do
		local fbody = "return function(self,t,val)\n\tif not val then val = {} end\n"
		local cbody = "return function(self,tuple)\n"
		cbody = cbody .. "\tlocal t if type(tuple) == 'userdata' then t = tuple:totable() else t = { tuple:unpack() } end\n"
		
		local s = {}
		local max = 1
		for k,v in pairs({"f_id","f_stt","f_pri","f_cnt","f_cnb", "f_runat"}) do
			-- print(k," ",v, " ",max)
			if self[v] then
				s[ self[v]+1 ] = v
				if max < self[v]+1 then max = self[v]+1 end
				-- print("set max to ", max, " for ",v)
			end
		end
		-- print ("max = ",max)
		for i = 1,max,1 do
			-- print("check ",i," ",s[i])
			if not s[i] then s[i] = false end
		end
		--table.sort(s)
		for k,v in ipairs(s) do
			-- print(k," ",v)
			if v then
				fbody = fbody .. "\tif #t < "..(k-1).." then box.raise(11,'tuple too short '..(#t)..' / '.."..(k-1)..") end\n"
				fbody = fbody .. "\ttable.insert(t, ".. (k) ..", val['"..v.."'] or self.def['".. v .."'])\n"
				
				cbody = cbody .. "\tt["..(k).."] = nil\n"
				-- fbody = fbody .. "\tprint(box.tuple.new(t))\n"
			end
		end
		fbody = fbody .. "\treturn t\nend\n"
		cbody = cbody .. "\treturn box.tuple.new(t)\nend\n"
		
		-- print("\n"..fbody)
		-- print("\n"..cbody)
		
		self.extend = box.dostring(fbody)
		self.collapse = box.dostring(cbody)
	end
	
	do
		local fbody = "return function(self,x) return "
		local count = 0
		for k,v in pairs( box.space[self.space].index[0].key_field ) do
			fbody = fbody .. 'x['.. v.fieldno ..'],'
			count = count + 1
		end
		fbody = fbody:sub(1,#fbody - 1) .. " end\n"
		--print(fbody)
		self.keyfield = box.dostring(fbody)
		self.keypack  = box.dostring("return function(self,x) return box.pack('"..string.rep("p",count).."',unpack(x)) end")
		self.keyunpack  = box.dostring("return function(self,x) return box.unpack('"..string.rep("p",count).."',x) end")
	end
	
	do
		local weak = setmetatable({}, { __mode = "kv" })
		weak.self = self
		self.watcher = box.fiber.wrap(function(w)
			box.fiber.name("q"..tostring(w.self.space)..".grd")
			while true do
				collectgarbage()
				if not w.self then break end
				
				local r,e = pcall(function(self)
					--print("loop 1 ",box.time64())
					local c = 0
					for kf,sid in pairs(self.taken) do
						-- print(k," -> ",v)
						if sid > 0 then
							if box.session.exists(sid) == 1 then
							else
								print("session ",sid, "was disconnected, but task ",box.tuple.new(kf)," was not released")
								self:on_disconnect(sid)
							end
						end
					end
				end, w.self)
				if not r then
					print(e)
				end
				
				collectgarbage()
				if not w.self then break end
				box.fiber.sleep(1)
			end
		end,weak)
	end
	if self.f_runat then
		local weak = setmetatable({}, { __mode = "kv" })
		weak.self = self
		local chan = box.ipc.channel(1)
		local maxwait = self.pause or 1
		self.runat_chan = chan
		self.runat = box.fiber.wrap(function(w)
			box.fiber.name("q"..tostring(w.self.space)..".run")
			--if true then return end
			local idx = box.space[w.self.space].index[ w.self.index_runat ]
			while true do
				collectgarbage()
				if not w.self then break end
				
				local r,e = pcall(function(self)
					--print("loop 2 ",box.time64())
					local collect = {}
					local it = idx:iterator(box.index.GT,0)
					for t in it do
						-- print("checking ",t)
						if box.unpack('l',t[ self.f_runat ]) <= box.time64() then
							table.insert(collect,t)
						else
							break
						end
						if #collect >= 1000 then break end
					end
					
					for _,t in ipairs(collect) do
						print("collect ",t)
						if t[self.f_stt] == 'W' then
							-- turn Wait into Ready
							print("Put task to ready ",box.tuple.new(self:keyfield(t)))
							local u = box.update(self.space, { self:keyfield(t) }, '=p=p', self.f_stt, 'R', self.f_runat, -1ULL)
							self:wakeup(u)
						elseif t[self.f_stt] == 'R' then
							print("Drop old task ",box.tuple.new(self:keyfield(t)))
							box.delete(self.space, { self:keyfield(t) } )
						elseif t[self.f_stt] == 'Z' then
							print("Kill zombie ",box.tuple.new(self:keyfield(t)))
							box.delete(self.space, { self:keyfield(t) } )
						elseif t[self.f_stt] == 'T' then
							-- Autorelease (TODO)
							print("Task taken too long ",box.tuple.new(self:keyfield(t)))
							box.update(self.space, { self:keyfield(t) }, '=p', self.f_runat, -1ULL)
						else
							print("Unsupported status ",t[self.f_stt], " in runat")
							box.update(self.space, { self:keyfield(t) }, '=p', self.f_runat, -1ULL)
						end
					end
					if #collect < 1000 then
						it = idx:iterator(box.index.GT,0)
						local n = it()
						if n then
							local v = box.unpack('l',n[ self.f_runat ])
							if v < -1ULL then
								print("Have next task after ",tonumber(v - box.time64())/1e6)
								return tonumber(v - box.time64())/1e6
							end
						end
					end
					return 1 -- no next task. sleep 1 second at most
				end, w.self)
				
				if not r then
					print(e)
				end
				
				collectgarbage()
				if not w.self then break end
				
				local z = chan:get(r and e or maxwait)
				if z then
					print("awake from channel")
				end
			end
		end,weak)
		
	end
	
	self:starttest()
	print ("Configured EQ on ",self,": sp:",self.space)
end

function M:starttest()
	local c = 0
	for v in box.space[self.space].index[self.index]:iterator(box.index.EQ, 'T') do
		c = c + 1
		local kf = { self:keyfield( v ) }
		box.update(self.space,kf,'=p',self.f_stt, 'R')
	end
	if c > 0 then
		print("Reset ",c," records from T ro R")
	end
end

function M:insert(...)
	local arg = {...}
	if #arg == 1 then arg = arg[1] end
	local t = box.insert(self.space, box.tuple.new(self:extend(arg)))
	self:wakeup(t)
	return t -- self:collapse(t)
end

function M:replace(...)
	local arg = {...}
	if #arg == 1 then arg = arg[0] end
	-- TODO: select, check taken, ...
	local t = box.tuple.new(self:extend(arg));
	box.replace(self.space,t)
end

function M:put(arg,opts)
	--local arg = {...}
	--if type(arg[1]) == 'table' then arg = arg[1] end
	opts = opts or {}
	
	local id
	if self.auto_id then
		local maxt = box.space[self.space].index[0]:max()
		if maxt then
			local maxv = box.unpack('l',maxt[ self.f_id ])
			local now = box.time64()
			if now > maxv then
				id = now
			else
				id = maxv+1
			end
		else
			id = box.time64()
		end
	end
	local ext = {
		f_id    = id;
		f_stt   = opts.status;
		f_pri   = opts.pri or opts.priority;
		f_runat = -1ULL;
	}
	
	if opts.delay then
		ext.f_stt = 'W'
		ext.f_runat = box.time64() + tonumber64( opts.delay * 1E6 )
	elseif opts.ttl then
		ext.f_runat = box.time64() + tonumber64( opts.ttl * 1E6 )
	end
	
	--print(box.tuple.new(arg))
	local tbl = self:extend(arg,ext)
	-- print("inserting ",box.tuple.new(tbl))
	local t = box.insert(self.space,box.tuple.new(tbl))
	if t[self.f_stt] == 'R' then
		self:wakeup(t)
	end
	if self.runat_chan and ext.f_runat ~= -1ULL then
		self.runat_chan:put(true,0)
	end
	return t
end

--[[

touch (complete_task_tuple, new_status)

turn task to any state

]]

function M:touch(task,status)
	local t = box.select(self.space,0,{ self:keyfield(task) })
	if t[self.f_stt] == 'T' then
		box.raise(box.error.ER_PROC_LUA,"Touching taken task not allowed")
	end
	t = box.update(self.space, { self:keyfield(task) }, '=p', self.f_stt, status)
	self:wakeup(t)
	return t
end

--[[

internal

wake waiting fibers

]]

function M:wakeup(t)
	if t[self.f_stt] ~= 'R' then return end
	for _,v in pairs(self.wait) do
		v:put(t,0)
		return
	end
	print("No waits")
end

--[[

take task with timeout

]]

function M:take(to)
	assert(to, "Timeout required")
	to = tonumber(to)
	local now = box.time()
	local _,t
	while true do
		_,t = box.space[self.space].index[self.index]:next_equal( 'R' )
		if t then
			break
		else
			local left = (now + to) - box.time()
			--print("left: ",left)
			if left <= 0 then
				return
			end
			local wseq = self.wseq
			self.wseq = wseq + 1
			
			local ch = box.ipc.channel(1)
			self.wait[wseq] = ch
			t = ch:get( left )
			self.wait[wseq] = nil
			if t then
				print("Got from channel, left: ",(now + to) - box.time(),": ",t)
				break
			end
		end
	end
	
	local sid = box.session.id()
	
	local kf = { self:keyfield( t ) }
	local kfi = self:keypack(kf)
	print("register ",box.tuple.new(kf)," for ",sid)
	t = box.update(self.space, kf, '=p', self.f_stt, 'T' )
	
	if not self.bysid[ sid ] then self.bysid[ sid ] = {} end
	self.taken[ kfi ] = sid
	self.bysid[ sid ][ kfi ] = true
	
	return t -- self:collapse(t)
end

function M:check_owner(kfi)
	if not self.taken[kfi] then
		local id = tostring(box.tuple.new({self:keyunpack(kfi)}))
		box.raise(11,string.format( "Task %s not taken by any", id ))
	end
	if self.taken[kfi] ~= box.session.id() then
		local id = tostring(box.tuple.new({self:keyunpack(kfi)}))
		box.raise(11,string.format( "Task %s taken by %d. Not you (%d)", id, self.taken[kfi], box.session.id() ))
	end
	return true
end

--[[

ack - confirm that task is done

arg1: full tuple or table with key
arg2: table with optional params

]]

function M:ack(key,opt)
	local kf = type(key) == 'userdata' and { self:keyfield(key) } or key
	local kfi = self:keypack(kf)
	self:check_owner(kfi)
	
	local sid = self.taken[ kfi ]
	self.taken[ kfi ] = nil
	self.bysid[ sid ][ kfi ] = nil
	
	-- return self:collapse( box.delete(self.space, kf) )
	if self.zombie and self.f_runat then
		self.runat_chan:put(true,0)
		return box.update(self.space, kf, '=p=p', self.f_stt, 'Z', self.f_runat, box.time64() + tonumber64( self.zombie*1e6 ) )
	else
		return box.delete(self.space, kf)
	end
end

function M:release(key,opt)
	local kf = type(key) == 'userdata' and { self:keyfield(key) } or key
	local kfi = self:keypack(kf)
	self:check_owner(kfi)
	opt = opt or {}
	
	if not opt.update then opt.update = { "" } end
	local update = {unpack(opt.update)}
	local up = table.remove(update,1)
	
	local t
	if opt.delay and self.runat then
		t = box.update(self.space, kf, '=p=p'..up, self.f_stt, 'W', self.f_runat, box.time64() + tonumber64( opt.delay*1e6 ), unpack(update) )
		self.runat_chan:put(true,0)
	else
		t = box.update(self.space, kf, '=p'..up, self.f_stt, 'R', unpack(update) )
		self:wakeup(t)
	end
	
	local sid = self.taken[ kfi ]
	self.taken[ kfi ] = nil
	self.bysid[ sid ][ kfi ] = nil
	
	
	return t -- self:collapse( t )
end

function M:done(key,opt)
	local kf = type(key) == 'userdata' and { self:keyfield(key) } or key
	local kfi = self:keypack(kf)
	self:check_owner(kfi)
	opt = opt or {}
	
	if not opt.update then opt.update = { "" } end
	local update = {unpack(opt.update)}
	local up = table.remove(update,1)
	--return self:collapse( box.update(self.space, kf, '=p'..up, self.f_stt, 'D', ... ) )
	local t = box.update(self.space, kf, '=p'..up, self.f_stt, 'D', unpack(update) )
	
	local sid = self.taken[ kfi ]
	self.taken[ kfi ] = nil
	self.bysid[ sid ][ kfi ] = nil
	
	return t
end

--function M:bury(kf, t, up, ...)
function M:bury(key,opt)
	local kf = type(key) == 'userdata' and { self:keyfield(key) } or key
	local kfi = self:keypack(kf)
	self:check_owner(kfi)
	opt = opt or {}
	
	if not opt.update then opt.update = { "" } end
	local update = {unpack(opt.update)}
	local up = table.remove(update,1)
	
	if opt.status and (
		#opt.status ~= 1 or
		opt.status == "R" or
		opt.status == "T" or
		opt.status == "W" or
		opt.status == "Z" or
		opt.status == "D"
	) then
		box.raise(box.error.ER_PROC_LUA,"Bad status for bury: "..opt.status)
	elseif not opt.status then
		opt.status = "B"
	end
	
	-- return self:collapse( box.update(self.space, kf, '=p'..up, self.f_stt, t, ... ) )
	local t = box.update(self.space, kf, '=p'..up, self.f_stt, opt.status, unpack(update) )
	
	local sid = self.taken[ kfi ]
	self.taken[ kfi ] = nil
	self.bysid[ sid ][ kfi ] = nil
	
	return t
end

return M

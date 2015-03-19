local log = require 'box.log'

local M = {}
M.__index = M

setmetatable(M,{
	__call = function(t,...) return t:new(...) end,
})

function M.new(class, host, port, opts)
	local self = setmetatable({},class)
	
	opts = opts or {}
	self.host = host
	self.port = port
	self.timeout = opts.timeout or 1
	self.max_delay = opts.max_wait or 1000
	
	return self
end

function M:send(key,value)
	local time = math.floor(box.time())
	key = key:gsub("/","-"):gsub("%s+","_")
	local row = key .. " " .. tostring(tonumber(value)).." "..tostring(time).."\n"
	
	if self.delay then
		if #self.delay > self.max_delay then return end
		table.insert(self.delay,row)
		return
	end
	
	if not self.s or box.time() - self.last_resolve > 60 then
		self.delay = {row}
		box.fiber.wrap(function()
			box.fiber.name("gr.udp/"..self.port..":"..self.host)
			local ai = box.socket.getaddrinfo( self.host, self.port, self.timeout, {
				['type'] = 'SOCK_DGRAM',
				protocol = 'udp',
			} )
			
			if ai and #ai > 0 then
			else
				log.crit("Resolve %s failed: %s",self.host, box.errno.strerror( box.errno() == 0 and box.errno.ENXIO or box.errno() ));
				return
			end
			local ainfo = ai[1]
			local s = box.socket( ainfo.family, ainfo.type, ainfo.protocol )
			if not s then
				log.crit("Socket %s:%s failed: %s",self.host, self.port, box.errno.strerror( box.errno() ));
				return
			end
			
			s:nonblock(true)
			s:linger(1,0)
			
				if s:sysconnect( ainfo.host, ainfo.port ) then
					self.s = s
					-- print("immediate connected")
				else
					if s:errno() == box.errno.EINPROGRESS
					or s:errno() == box.errno.EALREADY
					or s:errno() == box.errno.EWOULDBLOCK
					then
						local wr = s:writable(ctx.timeout)
						if wr then
							-- connected
						else
							log.crit("Connect %s:%s failed: %s",self.host, self.port, box.errno.strerror( box.errno.ETIMEDOUT ));
							return
						end
					else
						log.crit("Connect %s:%s failed: %s",self.host, self.port, box.errno.strerror( s:errno() ));
						return
					end
				end
				
				
				if not s:peer() then
					log.crit("Connect %s:%s failed: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
					return
				end
				
				-- log.info("connected to %s:%s (%s)",self.host,self.port,s:peer())
			
			self.last_resolve = box.time()
			
			self.s = s
			local delayed = self.delay
			self.delay = nil
			for _,row in ipairs(delayed) do
				local w = self.s:send(row)
				if not w then 
					log.crit("Failed to send to %s:%s: %s",self.host, self.port, box.errno.strerror( self.s:errno() ));
				end
			end
		end)
	else
		local w = self.s:send(row)
		if not w then 
			log.crit("Failed to send to %s:%s: %s",self.host, self.port, box.errno.strerror( self.s:errno() ));
		end
	end
end

return M

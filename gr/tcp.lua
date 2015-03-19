local log = require 'box.log'
local rel = require 'box.reload'

local M = {}
M.__index = M

setmetatable(M,{
	__call = function(t,...) return t:new(...) end,
})

function M:finish()
	self.ctx.work = false
	print("finising")
end

function M.new(class,host, port, opts)
	local self = setmetatable({},class)
	
	opts = opts or {}
	self.ctx = setmetatable({
		work          = true,
		host          = host,
		port          = port,
		timeout       = opts.timeout or 1,
		retry_timeout = opts.retry_timeout or 1/3.,
		max_wait      = opts.max_wait or 1000,
	},{ __mode = "v" })
	self.ctx.self = self
	
	-- rel:register(self,self.finish) -- ??
	
	self.fiber = box.fiber.wrap(function(ctx)
		box.fiber.name("gr.tcp/"..ctx.port..":"..ctx.host)
		while ctx.work do
			repeat
				local ai = box.socket.getaddrinfo( ctx.host, ctx.port, ctx.timeout, {
					['type'] = 'SOCK_STREAM',
				} )
				
				if ai and #ai > 0 then
				
				else
					log.crit("Resolve %s failed: %s",ctx.host, box.errno.strerror( box.errno() == 0 and box.errno.ENXIO or box.errno() ));
					box.fiber.sleep(ctx.retry_timeout)
					do break end
				end
				
				-- print("Resolved")
				
				local ainfo = ai[1]
				local s = box.socket( ainfo.family, ainfo.type, ainfo.protocol )
				if not s then
					log.crit("Connect %s:%s failed: %s",ctx.host, ctx.port, box.errno.strerror( box.errno() ));
					box.fiber.sleep(ctx.retry_timeout)
					do break end
				end
				
				s:nonblock(true)
				s:linger(1,0)
				
				if s:sysconnect( ainfo.host, ainfo.port ) then
					ctx.s = s
					print("immediate connected")
				else
					if s:errno() == box.errno.EINPROGRESS
					or s:errno() == box.errno.EALREADY
					or s:errno() == box.errno.EWOULDBLOCK
					then
						local wr = s:writable(ctx.timeout)
						if wr then
							print("connected")
							-- connected
						else
							log.crit("Connect %s:%s failed: %s",ctx.host, ctx.port, box.errno.strerror( box.errno.ETIMEDOUT ));
							do break end
						end
					else
						log.crit("Connect %s:%s failed: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
						box.fiber.sleep(ctx.retry_timeout)
						do break end
					end
				end
				
				
				if not s:peer() then
					local rbuf = s:sysread( 1 )
					log.crit("Connect %s:%s failed: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
					box.fiber.sleep(ctx.retry_timeout)
					do break end
				end
				
				log.info("connected to %s:%s (%s)",ctx.host,ctx.port,s:peer())
				
				ctx.s = s
				
				while true do
					box.fiber.testcancel()
					
					if not ctx.self then
						ctx.work = false
						break
					end
					
					if ctx.wbuf then
						--print("got delayed wbuf ")
						local w = s:writable(ctx.timeout)
						if w then
							local wbuf = table.concat(ctx.wbuf)
							print("send delayed ",wbuf)
							local wr = s:syswrite(wbuf)
							if wr then
								if wr == #wbuf then
									ctx.wbuf = nil
									print("finished write")
								else
									-- TODO: correct rebuffer
									ctx.wbuf = { wbuf:sub(wr+1) }
									print("have leftover ",#ctx.wbuf)
								end
							else
								log.crit("Connection to %s:%s reset: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
								break
							end
						else
							log.crit("Connection to %s:%s reset: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
							break
						end
					else
						local r = s:readable(ctx.timeout/5)
						if r then
							local rbuf = s:sysread( 256*1024 )
							if rbuf then
								log.warn("Received something from socket: %s",rbuf)
							else
								log.crit("Connection to %s:%s reset: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
								break
							end
						else
							if s:errno() == box.errno.ETIMEDOUT then
								if not ctx.work then break end
							else
								log.crit("Connection to %s:%s reset: %s",ctx.host, ctx.port, box.errno.strerror( s:errno() ));
								break
							end
						end
					end
				end
				
				ctx.s = nil
				s:close()
				s = nil
				
			until true
			print("leave fiber")
		end 
		
	end, self.ctx)
	
	return self
end

function M:send(key,value)
	local time = math.floor(box.time())
	key = key:gsub("/","-"):gsub("%s+","_")
	local row = key .. " " .. tostring(tonumber(value)).." "..tostring(time).."\n"
	self:write(row)
end

function M:write(row)
	local ctx = self.ctx
	if ctx.wbuf then
		print("delay ",row)
		if #ctx.wbuf > ctx.max_wait then
			log.warn("discard %d bytes by overflow",#row)
			return
		end
		table.insert(ctx.wbuf,row)
		return
	end
	if not ctx.s then
		print("notcon ",row)
		ctx.wbuf = { "",row }
		return
	end
	--local wr = ctx.s:syswrite(row:sub(1,10))
	local wr = ctx.s:syswrite(row)
	if wr then
		if wr == #row then
			--print("completely sent "..row)
			return
		else
			print("partial write failed ",wr," of ",#row)
			ctx.wbuf = { row:sub(wr+1) }
		end
	else
		log.crit("Connection to %s:%s reset: %s",ctx.host, ctx.port, box.errno.strerror( ctx.s:errno() ));
		ctx.wbuf = { "",row }
		return
	end
end

return M

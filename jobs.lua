local ct = require 'box.conntrack'

if not rawget(_G,'jobs') then
	rawset(_G,'jobs',{
		t = {
			-- type, type, ...
		}, -- available types
		w = {
			-- wid = { chan, sid, type, alias, peer }
		}, -- workers by id
		inprog = { -- tasks in progress
			-- tid = {ch,wid}
		},
		wrk = { -- array of active workers
			-- type = { id, id, ... }
		},
	})
end

function jobs:on_disconnect(sid)
	print("disconnected ",sid)
	for wid, inf in pairs( jobs.w ) do
		local wch, wsid, wtype = unpack(inf)
		if wsid == sid then
			print("drop worker ",wid, " of type ", wtype)
			wch:put(false,0)
			for _,i in pairs( jobs.wrk[ wtype ] ) do
				if i == wid then
					print("remove idx ",_)
					table.remove(jobs.wrk[ wtype ],_)
					break
				end
			end
			for tid,tinf in pairs( jobs.inprog ) do
				if tinf[2] == wid then
					tinf[1]:put(false,0)
				end
			end
		end
	end
end

ct.on_disconnect(jobs,jobs.on_disconnect)

function jobs.workers(...)
	for k,v in pairs({...}) do
		print("Allowed worker '",v,"' for jobs")
		jobs.t[ v ] = true -- setmetatable({},{__mode = "kv"})
		jobs.wrk[v] = jobs.wrk[v] or {}
	end
end

function jobs.worker(wtype, alias)
	wtype = wtype or 'any'
	alias = alias or 'unk'
	if not jobs.t[wtype] then
		box.raise(51, string.format("Worker type '%s' not allowed", tostring(wtype)))
	end
	local sid = box.session.id()
	print(string.format("Incoming worker '%s' as '%s' from %s (%s/%s)", wtype, alias, box.session.peer(), sid, box.fiber.id() ))
	
	
	local ch = box.ipc.channel(10)
	local wid = tostring(box.time64())
	jobs.w[wid] = { ch, sid, wtype, alias, box.session.peer() }
	table.insert(jobs.wrk[ wtype ], wid)
	return wid
end

function jobs.work(wid, timeout)
	timeout = timeout and tonumber(timeout) or 1
	if type(wid) ~= 'string' then wid = tostring(wid) end
	if not jobs.w[wid] then
		box.raise(51, string.format("Worker id '%s' not registered", wid))
	end
	local task = jobs.w[wid][1]:get(timeout)
	if task then
		return box.tuple.new(task)
	else
		return
	end
end

function jobs.done(tid, data)
	-- print("got task result: ",tid, " ", data)
	if not jobs.inprog[tid] then
		print(string.format("No task '%s' (timeout or error)", tostring(tid)))
		return
	end
	local res = jobs.inprog[tid][1]:put(data,0)
	if not res then
		print(string.format("Task '%s' result timeout", tostring(tid)))
		return
	end
end

function jobs.task(wtype,data,timeout)
	timeout = timeout and tonumber(timeout) or 1
	wtype = wtype or 'any'
	if not jobs.t[wtype] then
		box.raise(51, string.format("Task type '%s' not allowed", tostring(wtype)))
	end
	if #jobs.wrk[wtype] > 0 then
		-- print("Have workers ",table.concat(jobs.wrk[wtype],', '))
		local ch = box.ipc.channel(1)
		local tid = tostring(box.time64())
		local task = {tid,data}
		jobs.inprog[ tid ] = { ch, -1 }
		for _,wid in pairs(jobs.wrk[wtype]) do
			if jobs.w[wid][1]:has_readers() then
				local enq = jobs.w[wid][1]:put(task,0)
				if enq then
					local wch, wsid, wtype, alias, peer = unpack( jobs.w[wid] )
					print("Task passed to worker ",wid," known as ",alias," at ",peer)
					
					jobs.inprog[ tid ][2] = wid
					if #jobs.wrk[wtype] > 1 then
						table.remove(jobs.wrk[wtype], _)
						table.insert(jobs.wrk[wtype], wid)
					end
					local res = ch:get(timeout)
					jobs.inprog[ tid ] = nil
					if res then
						return res
					elseif res == nil then
						box.raise(51, "Request timed out")
					else
						box.raise(51, "Connection reset by peer")
					end
				end
			end
		end
		jobs.inprog[ tid ] = nil
		box.raise(51, string.format("No workers active for type '%s'", tostring(wtype)))
	else
		box.raise(51, string.format("No workers available for type '%s'", tostring(wtype)))
	end
end

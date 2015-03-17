local old = rawget(_G,'box.conntrack')

local M,D,C
if old then
	M = old
	D = M.D
	C = M.C
else
	D = setmetatable({}, { __mode = "kv" })
	C = setmetatable({}, { __mode = "kv" })

	M = setmetatable({
		D = D;
		C = C;
	}, {  })
end

box.fiber.ADM = box.fiber.ADM or {}

function box.fiber.is_admin()
	return box.fiber.ADM[ box.fiber.id() ] or false
end

box.session.on_disconnect(function()
	print("disconnected: ",box.session.peer()," sid=",box.session.id(), "; fid=",box.fiber.id() )
	local sid = box.session.id()
	for k,cb in pairs(D) do
		if k and cb then
			--print("notify ",k," with ",cb," about ",sid)
			local r,e = pcall(cb,k,sid)
			if not r then print(e) end
		elseif k then
			D[k] = nil
		end
	end
	box.fiber.ADM[ box.fiber.id() ] = nil
	collectgarbage()
end)

box.session.on_connect(function()
	box.fiber.ADM[ box.fiber.id() ] = not not box.fiber.name():match("^admin/")
	if box.acl then
		box.acl.connected()
	end
	print("connected: ",box.session.peer()," sid=",box.session.id(), "; fid=",box.fiber.id(),"; admin:", box.fiber.ADM[ box.fiber.id() ])
	
	local peer = box.session.peer()
	box.session.peer = function() return peer end
	
	local sid = box.session.id()
	for k,cb in pairs(C) do
		if k and cb then
			local r,e = pcall(cb,k,sid)
			if not r then print(e) end
		elseif k then
			C[k] = nil
		end
	end
	collectgarbage()
end)

function M.on_connect(ref,cb)
	C[ref] = cb
end
function M.on_disconnect(ref,cb)
	D[ref] = cb
end

rawset(_G,'box.conntrack',M)

return M
--[[
box.fiber.wrap(function()
	box.fiber.name("eq:gc")
	while true do
		collectgarbage()
		for k,v in pairs(D) do
			print(k,": ",v)
		end
		box.fiber.sleep(1)
	end
end)
]]

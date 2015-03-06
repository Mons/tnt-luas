local M = box.acl or {
	rules = {}
}

local clr = require 'devel.caller'
local ctr = require 'box.conntrack'
box.acl = M

local function inet_ntoa(addr)
	if type( addr ) ~= "number" then return nil end
	
	local n4 = bit.band(bit.rshift(addr, 0),  0x000000FF)
	local n3 = bit.band(bit.rshift(addr, 8),  0x000000FF)
	local n2 = bit.band(bit.rshift(addr, 16), 0x000000FF)
	local n1 = bit.band(bit.rshift(addr, 24), 0x000000FF)
	
	return string.format("%d.%d.%d.%d", n1, n2, n3, n4)
end

function netmask(mask)
	-- fucking lua eats my mind
	-- bit.rshift(bit.tobit(0xffffffff),31) == 1
	-- but
	-- bit.rshift(bit.tobit(0xffffffff),32) == 0xffffffff or -1
	-- workaround for 32
	if mask == 0 then return bit.tobit(0) end
	--if mask == 32 then return bit.tobit(0xffffffff) end
	
	return bit.lshift(bit.rshift(bit.tobit(0xffffffff), 32-mask),32-mask)
end

local function inet_aton(str)
	local o4, o3, o2, o1, mask = str:match('^(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)$');
	if o4 and o3 and o2 and o1 then
		return 2^24*o4 + 2^16*o3 + 2^8*o2 + o1;
	else
		return nil
	end
end

local function inet_cidr(str)
	local o4, o3, o2, o1, mask = str:match('^(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)%.(%d%d?%d?)/(%d%d?)$');
	if o4 and o3 and o2 and o1 and mask then
		return (2^24*o4 + 2^16*o3 + 2^8*o2 + o1), tonumber(mask);
	else
		return nil
	end
end

local function ipv4_network(ip_address, netmask)
  return math.floor(ip_address / 2^(32-netmask));
end

local function ipv4_in_network(ip_address, network, netmask)
  return ipv4_network(ip_address, netmask) == ipv4_network(network, netmask);
end

function M.connected()
	local host = box.session.peer():match("^([^:]+)")
	local addr = inet_aton(host)
	local lo = bit.band(addr,0xff000000) == 0x7f000000
	-- print(host, " -> ",addr, " admin: ",box.fiber.is_admin())
	for i,r in ipairs(M.rules) do
		if box.acl.debug then
			print("check ",host," for ", inet_ntoa(r.addr),"/",r.mask,"/",inet_ntoa(r.net), " (",bit.tohex(bit.band(addr,r.net)), " : ",bit.tohex(r.net),")")
		end
		if bit.band(addr,r.net) == r.addr then
			if r.acl then
				print(string.format("host %s allowed by rule #%d %s", host, i, r.rule))
				break
			else
				if lo and box.fiber.is_admin() then
					print(string.format("Localhost admin connection denied by rule #%d %s (%s/%s). Ignoring", i, r.rule, inet_ntoa(r.addr),r.mask))
				else
					print(string.format("host %s denied by rule #%d %s (%s/%s)", host, i, r.rule, inet_ntoa(r.addr),r.mask))
					box.fiber.sleep(1/3)
					box.raise(51,string.format("host %s denied by rule #%d %s (%s/%s)", host, i, r.rule, inet_ntoa(r.addr),r.mask))
				end
				break
			end
		end
	end
	if M.default ~= nil then
		if M.default then
			print(string.format("host %s allowed by default", host))
		elseif lo and box.fiber.is_admin() then
			-- accept admin localhost
		else
			print(string.format("host %s denied by default", host))
			box.fiber.sleep(1/3)
			box.raise(51,string.format("host %s denied by default", host))
		end
	end
end

--ctr.on_connect(M,M.connected)

function M._access(acl,h)
	h = h and tostring(h) or '0.0.0.0/0'
	local addr,mask
	if h == '*' or h == 'all' then
		if #M.rules == 0 and not acl then
			box.raise(51,"Denied all connections "..clr(1))
		end
		addr,mask = 0,0
	elseif h:match(".+/.+") then
		addr,mask = inet_cidr(h)
		if not addr then
			box.raise(51,string.format("Bad address string '%s'%s",h,clr(1)))
		end
	else
		addr = inet_aton(h)
		mask = 32
		if not addr then
			box.raise(51,string.format("Bad address string '%s'%s",h,clr(1)))
		end
	end
	local net = netmask(mask)
	print("acl ",acl," ",bit.tohex(addr),"/",mask," as ",bit.tohex(bit.band(addr,net))," / ",bit.tohex(net))
	table.insert(M.rules,{
		rule = h,
		addr = bit.band(addr,net),
		mask = mask,
		net  = net,
		acl  = acl
	})
end

function M.allow(h)
	return M._access(true,h)
end

function M.deny(h)
	return M._access(false,h)
end

return M
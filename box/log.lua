-- box = box or {}

require 'box.conntrack'

local io = require 'io'
local myid = box and box.cfg and box.cfg.bind_ipaddr..':'..box.cfg.primary_port or 'lua'

local M = {
	ALERT = 1,
	CRIT  = 2,
	ERR   = 3,
	WARN  = 4,
	NOTICE= 5,
	INFO  = 6,
	DEBUG = 7,
}

local REV = {}
for k,v in pairs(M) do
	REV[v] = k:sub(1,1)
end

--box.log = M

M.level = M.INFO
M.caller = true
M.callfunc = true

local trimre = {}
for pp in string.gmatch(package.path, "([^;]+)") do
	pp = pp:match('^([^%?]+)/[^/]')
	pp = string.gsub(pp,'([%^%$%(%)%%%.%[%]%*%+%-%?])','%%%1')
	table.insert(trimre,'^'..pp..'/')
end

local modlevels = {}

function M.modlevel(level)
	local caller = debug.getinfo(2)
	assert(REV[level],"Unknown level: "..level.." from "..caller.short_src..':'..tostring(caller.currentline))
	-- print("set modlevel for ",caller.short_src)
	modlevels[ caller.short_src ] = level
end

function M._log(level, msg, ...)
	local caller = debug.getinfo(3)
	if modlevels[ caller.short_src ] then
		if level > modlevels[ caller.short_src ] then return end
	else
		if level > M.level then return end
	end
	msg = tostring(msg)
	--assert(false)
	if string.match(msg,'%%') then
		msg = string.format(msg,...)
	else
		for _,v in pairs({...}) do
			msg = tostring(msg) .. ' ' .. tostring(v)
		end
		--msg = table.concat({msg,...},' ')
	end
	local callinfo = ''
	if M.caller then
		--for k,v in pairs(caller) do print("\t",k,"\t",v) end
		
		local line = caller.short_src
		for _,trim in pairs(trimre) do
			--print("trim", trim)
			if line:match(trim) then
				line = line:gsub(trim,'')
				break
			end
		end
		--print(line)
		--assert(false)
		callinfo = ' (@'..line..':'..tostring(caller.currentline)
		if M.callfunc then
			if caller.name then
				callinfo = callinfo .. '::' .. caller.name .. '@' .. tostring(caller.linedefined)
			else
				callinfo = callinfo .. '::' .. caller.what
			end
		end
		callinfo = callinfo ..')'
	end
	--io.stderr:write(myid .. ' ['..REV[level]..'] '..msg..callinfo.."\n")
	local message = myid..' ['..REV[level]..'] '..msg..callinfo
	print(message)
	if box.fiber.is_admin() then
		local name = box.fiber.name()
		box.fiber.wrap(function () box.fiber.name(name) print(message) end )
	end
	--[[
	io.stderr:write(string.format(
		'%s %-24s [%s] %s%s\n',
		myid,box.fiber.name(),
		REV[level],
		msg,
		callinfo
	))
	]]--
	--myid .. ' ['..REV[level]..'] '..msg..callinfo.."\n")
	--print(string.format('[%s] '..msg, REV[level],...)..callinfo )
end

function M.debug(...) M._log(M.DEBUG,...) end
function M.info(...) M._log(M.INFO,...) end
function M.notice(...) M._log(M.NOTICE,...) end
function M.warn(...) M._log(M.WARN,...) end
function M.err(...) M._log(M.ERR,...) end
M.error = M.err
function M.crit(...) M._log(M.CRIT,...) end
function M.alert(...) M._log(M.ALERT,...) end
function M.emerg(...) M._log(M.ALERT,...) end

return M

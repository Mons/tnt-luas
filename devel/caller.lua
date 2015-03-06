local trimre = {}
for pp in string.gmatch(package.path, "([^;]+)") do
	pp = pp:match('^([^%?]+)/[^/]')
	pp = string.gsub(pp,'([%^%$%(%)%%%.%[%]%*%+%-%?])','%%%1')
	table.insert(trimre,'^'..pp..'/')
end

return function (n)
	if n then n = tonumber(n) else n = 0 end
	local caller = debug.getinfo(2 + n)
	if not caller then return ' (@UNKNOWN)' end
		local callinfo = ''
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
		--if M.callfunc then
			if caller.name then
				callinfo = callinfo .. '::' .. caller.name .. '@' .. tostring(caller.linedefined)
			else
				callinfo = callinfo .. '::' .. caller.what
			end
		--end
		callinfo = callinfo ..')'
	return callinfo
end

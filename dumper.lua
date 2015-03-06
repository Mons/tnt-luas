local function _dumper(seen,t)
	if type(t) == 'table' then
		if seen[ t ] then
			return '\\'..tostring(t)
		end
		seen[ t ] = true
		local keys = 0
		for _,_ in pairs(t) do keys = keys + 1 end
		if keys ~= #t then
			local sub = {}
			local prev = 0
			for k,v in pairs(t) do
				if type(k) == 'number' and k == prev + 1 then
					prev = k
					table.insert(sub,_dumper(seen,v))
				else
					table.insert(sub,tostring(k)..'='.._dumper(seen,v))
				end
			end
			return '{'..table.concat(sub,'; ')..'}'
		else
			local sub = {}
			for _,v in ipairs(t) do
				table.insert(sub,_dumper(seen,v))
			end
			return '{'..table.concat(sub,', ')..'}'
		end
	elseif type(t) == 'number' then
		return tostring(t)
	elseif type(t) == 'string' then
		return "'" .. t .. "'"
	else
		return tostring(t)
	end
end

local function dumper(x)
	return _dumper({},x)
end

rawset(_G, 'dumper', dumper)

return dumper

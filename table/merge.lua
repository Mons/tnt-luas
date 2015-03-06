function table.merge(h1,h2)
	for k,v in pairs( h2 ) do
		if not h1[k] or type(h1[k]) ~= 'table' or type(h2[k]) ~= 'table' then
			if h2[k] == NULL then
				h1[k] = nil
			else
				h1[k] = h2[k]
			end
		else
			h1[k] = table.merge(h1[k],h2[k])
		end
	end
	return h1
end

if math.machine_epsilon == nil then
	math.machine_epsilon = 1.0
	while (1.0 + math.machine_epsilon / 2.0 > 1.0) do
		math.machine_epsilon = math.machine_epsilon / 2
	end
end

math.round = function(num, acc, limit)
	limit = 1 - (limit or 0.5)
	if limit == 1 then
		limit = 1 - math.machine_epsilon * 10 ^ (acc + math.log(num)/math.log(10))
	end
	acc = 10 ^ (acc or 0)
	return math.floor(num * acc + limit) / acc
end

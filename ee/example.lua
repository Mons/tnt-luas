require 'test'
function yield()
	box.fiber.sleep(0)
end

box.fiber.wrap( function()

require 'ee'
local d = require 'dumper'

local calls = {}

local first = ee:on('test', function(c,...)
	print("handler 1 fired ",...)
	table.insert(calls,{ 1, {...} })
end, 'first')

ee:event("test", 1,3,5) yield()

is_deeply(calls, {{1,{1,3,5}}}, "single call") calls = {} yield()

local second = ee:on('test', function(c, ...)
	print("handler 2 fired ",...)
	table.insert(calls,{ 2, {...} })
	c.stop()
end,'second')

ee:event("test", 4,"ttt") yield()
is_deeply(calls, { {2,{4,"ttt"}} }, "second + stop") calls = {} yield()

ee:once('test', function(c,...)
	print("handler 3 fired ",...)
	table.insert(calls,{ 3, {...} })
end,'third')

yield()


ee:event("test", 1,2,3) yield()

is_deeply(calls, { {3,{1,2,3}},{2,{1,2,3}} }, "first calls 3,2") calls = {} yield()

ee:event("test", 1,2,3) yield()

is_deeply(calls, { {2,{1,2,3}} }, "only second") calls = {} yield()

second()

ee:event("test", 1,2,3) yield()

is_deeply(calls, { {1,{1,2,3}} }, "only first") calls = {} yield()

first()

ee:event("test", 1,2,3) yield()

is_deeply(calls, { }, "no calls") calls = {} yield()

-- empty

local first = ee:on('test', function(c,...)
	print("handler 1 fired ",...)
	table.insert(calls,{ 1, {...} })
end, 'first')

-- print(ee.test)

ee.test(7,"xt") yield()

is_deeply(calls, { {1,{7,"xt"}} }, "by index") calls = {} yield()

ee.test(8,"ttt") yield()

is_deeply(calls, { {1,{8,"ttt"}} }, "by index (2)") calls = {} yield()

ee:no('test')

ee:event("test", 1,2,3) yield()

is_deeply(calls, { }, "no calls") calls = {} yield()


done_testing()

end)

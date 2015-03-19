box.fiber.wrap( function()

local y = ee:on('test', function(...)
	print("handler 1 fired ",...)
end)

local x = ee:on('test', function(c, ...)
	print("handler 2 fired ",...)
	c.stop()
end)

ee:once('test', function(...)
	print("handler 3 fired ",...)
end)


ee:event("test", 1,2,3)
box.fiber.sleep(0.001)
ee:event("test", 4)
box.fiber.sleep(0.001)
x()
box.fiber.sleep(0.001)
ee:event("test", 5)
box.fiber.sleep(0.001)
y()
box.fiber.sleep(0.001)
ee:event("test", 6)


end)

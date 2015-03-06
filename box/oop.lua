local base = _G
module("box.oop", package.seeall)

local seq = 0
local function _constructor(cls,...)
	seq = seq + 1
	local self = base.setmetatable({ _ = {}, __id = seq }, cls)
	self:init(...)
	return self
end

local object = {
	__name = "object",
	__tostring = function(self)
		return self:stringify()
	end
}

object.__index = object

base.setmetatable(object, {
	__name = "object_mt";
	__call = _constructor;
})

function object:init(...)
	--print("init: ",self.__name,' ',...)
end

function object:stringify()
	return "object:"..self.__name..':'..self.__id
end

function object.inherits(from, newc)
	-- print("inherits: ",from," <- ", newc)
	newc.__index = newc;
	base.setmetatable(newc,{
		__index = from;
		__call  = base.rawget(base.getmetatable(from),'__call');
	})
	for k,v in base.pairs(from) do
		if not base.rawget(newc,k) and k:match("^__") then
			newc[k] = from[k]
		end
	end
	return newc;
end

function object:define(newc)
	return object.inherits(self,newc)
end

function object:new(...)
	return self(...)
end


return object

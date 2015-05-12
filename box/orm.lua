local object = require 'box.oop'

local Tuple = object:define({ __name = 'Tuple'})

function Tuple:init(sp,t)
	self.__space = sp
	for k,v in pairs( sp:tuple2hash( t ) ) do
		self[k] = v
	end
	return self
end

function Tuple:totuple()
	return self.__space:hash2tuple(self)
end

function Tuple:stringify()
	local rv = self.__name..'{'
	for k,v in ipairs(self.__space.fields) do
		local val
		if type(self[v]) == 'table' then
			--if rawget(self[v],'__tostring') ~= nil then -- raises unknown exception o.O
			if self[v].__tostring ~= nil then
				val = tostring(self[v])
			else
				val = 'J:'..box.cjson.encode(self[v])
			end
		else
			val = tostring(self[v])
		end
		rv = rv .. v ..'='..val..';'
	end
	rv = rv .. '}'
	return rv
end

local function _pack(t, v, o)
	local major = t:sub(1, 1)
	local minor = t:sub(2, 2)
	local opt = t:sub(3)
	if minor == '@' then
		v = o:pack(opt, v)
		if major == 'i' then
			return tonumber(v)
		elseif major == 'l' then
			return tonumber64(v)
		else
			return v
		end
	end
	if major == 'l' then
		return box.pack(major, tonumber64(v or 0) )
	elseif major == 'i' then
		return box.pack(major, tonumber(v or 0) )
	elseif major == 'j' then
		return box.cjson.encode( v or {} )
	elseif major == 'p' and minor == 'i' then
		if opt:len() > 0 then
			return string.format(opt, tonumber(v or 0) )
		else
			return tostring( tonumber(v or 0) )
		end
	else
		return tostring(v or '')
	end
end

local function _unpack (t, v, o)
	local major = t:sub(1, 1)
	local minor = t:sub(2, 2)
	local opt = t:sub(3)
	if minor == '@' then
		if major == 'i' or major == 'l' then
			v = box.unpack(major, v)
		end
		return o:unpack(opt, v)
	end
	if major == 'l' then
		return box.unpack(major, v)
	elseif major == 'i' then
		return box.unpack(major, v)
	elseif major == 'j' then
		local r,v = pcall(box.cjson.decode,v)
		if not r then
			box.log.error("Failed to decode json from "..t..": "..v)
			v = {}
		end
		return v
	elseif major == 'p' and minor == 'i' then
		return tonumber( v )
	else
		return v
	end
end

function Tuple:update(f,...)
	local update = {}
	local _
	for k,v in ipairs({...}) do
		if k % 2 ~= 0 then
			if type(v) ~= 'number' then
				_ = self.__space.f2id[ v ]
			else
				_ = v
			end
			table.insert( update, _)
		else
			table.insert( update, _pack(self.__space.types[_ + 1], v, self))
		end
	end
	
	local t = box.update(
		self.__space.id,
		self.__space:hash2ituple(0,self),
		f,
		unpack(update)
	)
	--print(t)
	for k,v in pairs( self.__space:tuple2hash( t ) ) do
		self[k] = v
	end
	return self
end

function Tuple:delete()
	return box.delete(
		self.__space.id,
		self.__space:hash2ituple(0,self)
	)
end

function Tuple:replace()
	return self.__space:tuple2hash(box.replace(
		self.__space.id,
		self.__space:hash2tuple(self)
	))
end

local Space = object:define({ __name = 'Space'})

function Space:hash2tuple(h)
	local t = {}
	local types = self.types
	for _,k in ipairs( self.fields ) do
		table.insert(t, _pack(types[_], h[ k ], self.class))
	end
	local tuple = box.tuple.new(t)
	return tuple
end

function Space:hash2ituple(i,h)
	local t = {}
	local types = self.idx[i].types
	for _,k in ipairs( self.idx[i].fields ) do
		table.insert(t, _pack(types[_], h[ k ], self.class))
	end
	local tuple = box.tuple.new(t)
	-- print(tuple)
	return tuple
end

function Space:tuple2hash(t)
	local h = {}
	local types = self.types
	for _,k in ipairs( self.fields ) do
		if #t < _ then break end
		if t[ _-1 ] ~= nil then
			h[k] = _unpack(types[_], t[ _-1 ], self.class)
		end
	end
	return h
end

function Space:init(v)
	print("create space ",v.id)
	self.id = v.id or error("Space id required")
	self.__id = self.id
	assert(box.space[self.id].enabled, 'space '..self.id..' not enabled')
	self._ = box.space[self.id]
	self.name   = v.name
	self.fields = v.fields or error("Fields required")
	self.types  = v.types or error("Types required")
	self.class  = v.class or Tuple
	
	self.f2id = {}
	local n2t = {}
	for f,n in ipairs(v.fields) do
		self.f2id[ n ] = f-1
		n2t[ n ] = self.types[ f ]
	end
	
	self.idx = {}
	local this = self
	
	for index_id,i in pairs(self._.index) do
		-- todo i.key_field.type
		local name = {}
		local fields = {}
		local types = {}
		for _,f in pairs(i.key_field) do
			--print("index ",i,"; field ",f.fieldno, " -> ", v.fields[ f.fieldno+1 ])
			table.insert(fields,self.fields[ f.fieldno+1 ])
			table.insert(types,self.types[ f.fieldno+1 ])
		end
		local nm = table.concat(fields,'_')
		print("configure index ",nm," for ",self.name)
		assert( not self.idx[nm], 'index '..nm..' already exists' )
		
		
		self.idx[nm] = setmetatable({
			id     = index_id,
			fields = fields,
			types  = types,
			min    = function (self,...)
				local v = i:min(...)
				if v then
					v = this.class(this, v)
				end
				return v
			end,
			max    = function (self,...)
				local v = i:max(...)
				if v then
					v = this.class(this, v)
				end
				return v
			end,
			iterator = function (self, ...)
				local idx = i:iterator(...)
				return function()
					local v = idx()
					if v then
						v = this.class(this, v)
					end
					return v
				end
			end
		},{
			__index = i,
		})
		self.idx[nm].ALL = function (self, ...)
			return self:iterator(box.index.ALL, ...)
		end
		self.idx[nm].EQ = function (self, ...)
			return self:iterator(box.index.EQ, ...)
		end
		self.idx[nm].GT = function (self, ...)
			return self:iterator(box.index.GT, ...)
		end
		if i.type == 'TREE' then
			self.idx[nm].REQ = function (self, ...)
				return self:iterator(box.index.REQ, ...)
			end
			self.idx[nm].GE = function (self, ...)
				return self:iterator(box.index.GE, ...)
			end
			self.idx[nm].LT = function (self, ...)
				return self:iterator(box.index.LT, ...)
			end
			self.idx[nm].LE = function (self, ...)
				return self:iterator(box.index.LE, ...)
			end
		end
		if i.type == 'BITSET' then
			self.idx[nm].BITS_ALL_SET = function (self, ...)
				return self:iterator(box.index.BITS_ALL_SET, ...)
			end
			self.idx[nm].BITS_ANY_SET = function (self, ...)
				return self:iterator(box.index.BITS_ANY_SET, ...)
			end
			self.idx[nm].BITS_ALL_NOT_SET = function (self, ...)
				return self:iterator(box.index.BITS_ALL_NOT_SET, ...)
			end
		end
		self.idx[index_id] = self.idx[nm]
	end
	
end

function Space:_key(i,...)
	local key = {...}
	if #key > 0 and type(...) == 'table' then
		key = ...
		if key[0] then
		else
			local k1 = {}
			for _,k in ipairs( self.idx[i].fields ) do
				if key[k] then
					table.insert( k1, key[k] )
				else
					break
				end
			end
			key = k1
		end
	end
	
	local types = self.idx[i].types
	for _,t in ipairs( types ) do
		if key[_] then
			key[_] = _pack( t, key[_], self.class )
		else
			break
		end
	end
	return unpack(key)
end

function Space:select(index,...)
	assert(index, ":select requires index as first arg");
	local idx = type(index) == 'number' and index or self.idx[index].id
	local list = {box.select(
		self.id,
		idx,
		self:_key(index,...)
	)}
	local rv = {}
	for _,t in ipairs(list) do
		table.insert(rv, self.class( self, t ))
	end
end

function Space:limit_offset(index,limit,offset,...)
	assert(index, ":limit_offset requires index as first arg");
	assert(limit, ":limit_offset requires limit as second arg");
	assert(offset, ":limit_offset requires offset as third arg");
	local idx = type(index) == 'number' and index or self.idx[index].id
	local it = box.space[self.id].index[idx]:iterator(box.index.EQ, self:_key(idx,...))
	local got = 0
	local ret = {}
	for t in it do
		table.insert(ret,t)
		if offset > 0 then
			offset = offset - 1
			table.remove(ret,1)
		end
		if #ret >= limit then break end
	end
	for _,t in ipairs(ret) do
		ret[_] = self.class( self, t )
	end
	return unpack(ret)
end

function Space:one(index,...)
	local idx = type(index) == 'number' and index or self.idx[index].id
	assert(self.idx[idx].unique, "Non-unique index for select")
	assert(#self.idx[idx].fields == select('#',...), "Number of key fields mismatch")
	
	local one = box.select(
		self.id,
		idx,
		self:_key(index,...)
	)
	if one then
		return self.class( self, one )
	else
		return
	end
end

function Space:pairs()
	local gen,prm,st = self._:pairs()
	local this = self
	return function(...)
		local it,val = gen(...)
		if val then
			 val = this.class( this, val )
		end
		return it, val
	end, prm, st
end

function Space:delete(...)
	local t
	if type(...) == 'table' then
		t = box.delete(
			self.id,
			self:hash2ituple(0,...)
		)
	else
		t = box.delete(
			self.id,
			...
		)
	end
	if not t then return end
	return self.class( self, t )
end

function Space:insert(...)
	local t
	if type(...) == 'table' then
		t = box.insert(
			self.id,
			self:hash2tuple(...)
		)
	else
		t = box.insert(
			self.id,
			...
		)
	end
	if not t then return end
	return self.class( self, t )
end

function Space:replace(...)
	local t
	if type(...) == 'table' then
		t = box.replace(
			self.id,
			self:hash2tuple(...)
		)
	else
		t = box.replace(
			self.id,
			...
		)
	end
	if not t then return end
	return self.class( self, t )
end

function Space:len(...)
	return box.space[self.id]:len()
end

function Space:update(...)
	error("Not implemented yet")
end

local M = object:define({ __name = 'orm' })
M.Tuple = Tuple

function M:init()
	self.space = {}
end

function M.configure(cf)
	local O = M()
	
	for k,v in pairs(cf) do
		v.id = k
		O.space[ v.name ] = Space(v)
	end
	return O
end

function dots2hash(...)
	local r = {}
	local data = {...}
	--print("data = ", #data)
	for i = 1, #data, 2 do
		--if data[i] ~= 'data1' and data[i] ~= 'data2' and data[i] ~= 'data3' then print("assign ",i," ", data[ i ], " = ", data[i+1]) end
		--print("data = ", i, " ", data[i])
		r[ data[ i ] ] = data[ i + 1 ]
	end
	return r
end



return M
